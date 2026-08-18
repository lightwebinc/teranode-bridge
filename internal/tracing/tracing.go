// Package tracing initialises OpenTelemetry tracing for the bridge, matching
// Teranode's tracing setup (teranode/util/tracing) so a trace crosses the
// bridge instead of ending at it.
//
// # Why the bridge has to be traced
//
// The bridge sits on three boundaries Teranode already traces:
//
//   - outbound HTTP to the propagation service. Propagation extracts trace
//     context from request headers; a bridge that sends none makes every
//     transaction it forwards begin a NEW root span, so the wide-area leg is
//     invisible and the cluster-side span looks like it appeared from nowhere.
//   - outbound gRPC to the blockchain service, for the reverse path.
//   - inbound HTTP on the retrieval plane. The cluster's fetch of a subtree or
//     block happens INSIDE its block-validation span. Without extraction here,
//     "why was this block slow to validate" dead-ends at the bridge — which is
//     the single question an operator is most likely to ask of a landing shim.
//
// # Configuration parity
//
// The knobs carry Teranode's names and defaults: tracing is OFF unless enabled,
// the sample rate defaults to 0.01, and the collector defaults to a local OTLP
// HTTP endpoint. Sampling is ParentBased(TraceIDRatioBased(rate)) exactly as
// Teranode configures it, so a sampled cluster trace stays sampled across the
// bridge rather than being re-diced at the boundary.
//
// # Known gap, shared with upstream
//
// Announcements ride Kafka, and Teranode does not propagate trace context over
// Kafka either (see teranode/util/tracing/PROPAGATION.md, "Known Gaps"). The
// bridge does not invent a scheme of its own: when upstream adds a trace_context
// field to its Kafka messages, this package must carry it too, or the bridge
// becomes the only remaining hole in an otherwise connected trace.
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// Config mirrors Teranode's tracing_* settings.
type Config struct {
	// Enabled turns tracing on. Off by default, as upstream.
	Enabled bool
	// CollectorURL is the OTLP HTTP endpoint, e.g. http://localhost:4318.
	// An http scheme selects an insecure exporter, as upstream.
	CollectorURL string
	// SampleRate is the head sampling ratio for root spans. Child spans
	// inherit the parent's decision.
	SampleRate float64
	// ServiceName, Version and Instance become resource attributes, and must
	// match what the logging and metrics packages use so traces join them.
	ServiceName string
	Version     string
	Instance    string
}

// Init installs the global tracer provider and propagator, returning a shutdown
// function. When cfg.Enabled is false it installs the W3C propagator only and
// returns a no-op shutdown: spans become no-ops via the default provider, but
// incoming trace context is still parsed and forwarded, so a disabled bridge
// does not BREAK a trace it merely declines to contribute to.
func Init(ctx context.Context, cfg Config, log *slog.Logger) (func(context.Context) error, error) {
	// The composite propagator is installed either way: TraceContext for W3C
	// traceparent, Baggage for the accompanying baggage header. Teranode
	// installs the same pair.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	u, err := url.Parse(cfg.CollectorURL)
	if err != nil {
		return nil, fmt.Errorf("tracing: parse collector url %q: %w", cfg.CollectorURL, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("tracing: collector url %q has no host", cfg.CollectorURL)
	}

	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(u.Host)}
	if u.Scheme == "http" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("tracing: otlp exporter: %w", err)
	}

	// The identity triple only — the same three attributes logging.Init and the
	// metrics package attach, so a trace joins a log line and a series on
	// shared dimensions. Teranode also carries a `commit` attribute from a
	// separate build variable; the bridge has no such variable (its Version is
	// a `git describe`), and stamping the version under a `commit` key would be
	// a wrong answer rather than a missing one.
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceNameKey.String(cfg.ServiceName),
		semconv.ServiceVersionKey.String(cfg.Version),
		semconv.ServiceInstanceIDKey.String(cfg.Instance),
	))
	if err != nil {
		return nil, fmt.Errorf("tracing: resource: %w", err)
	}

	rate := cfg.SampleRate
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(rate))),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	log.Info("tracing enabled", "collector", cfg.CollectorURL, "sample_rate", rate,
		"service", cfg.ServiceName)

	return func(sctx context.Context) error {
		sctx, cancel := context.WithTimeout(sctx, 5*time.Second)
		defer cancel()
		return tp.Shutdown(sctx)
	}, nil
}
