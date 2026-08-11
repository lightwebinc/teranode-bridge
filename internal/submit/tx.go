// Package submit hands delivered transactions to the Teranode cluster.
//
// It uses the propagation service's HTTP endpoint (POST /tx) rather than its
// gRPC API, for three reasons that all matter to a bridge:
//
//   - The body is the raw transaction — exactly the bytes that arrive on the tx
//     lane — so nothing is re-encoded.
//   - HTTP classifies errors correctly. Over gRPC every validator failure is
//     flattened into one opaque PROCESSING/Internal, so a duplicate is
//     indistinguishable from a real rejection; over HTTP an already-known
//     transaction is a plain 200 and each failure class has its own status.
//   - It needs no generated stubs, so the bridge does not link the cluster's
//     module (448 dependencies) to send a byte slice.
//
// Extended format is accepted and preserved; the fabric delivers EF, so the
// transaction reaches the validator with its prevout data intact.
package submit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Outcome classifies what the cluster did with a transaction.
type Outcome int

const (
	// Accepted — the cluster took it (HTTP 200).
	Accepted Outcome = iota
	// Duplicate — already known. Success for our purposes: re-delivery after an
	// A/B failover or a reconnect must not read as an error.
	Duplicate
	// Rejected — the cluster refused it on its merits (bad tx, missing parent,
	// conflicting spend). Not retryable; the object is dropped.
	Rejected
	// Failed — transport or server fault. Retryable.
	Failed
)

func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "accepted"
	case Duplicate:
		return "duplicate"
	case Rejected:
		return "rejected"
	default:
		return "failed"
	}
}

// Config points the submitter at the cluster.
type Config struct {
	// Endpoints are propagation HTTP base URLs (e.g. http://192.0.2.10:20833).
	// More than one is round-robined, which is how the bridge spreads load
	// across a multi-node cluster or a VIP without any per-object work.
	Endpoints []string
	Timeout   time.Duration
}

// Submitter posts transactions to the cluster.
type Submitter struct {
	endpoints []string
	client    *http.Client
	log       *slog.Logger

	next                                  atomic.Uint64
	accepted, duplicate, rejected, failed atomic.Uint64
}

// New returns a submitter.
func New(cfg Config, log *slog.Logger) (*Submitter, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("submit: no propagation endpoints configured")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	eps := make([]string, 0, len(cfg.Endpoints))
	for _, e := range cfg.Endpoints {
		eps = append(eps, strings.TrimRight(e, "/"))
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// Long-lived, reused connections per endpoint: the tx lane is a steady
	// stream, and a fresh dial per transaction would dominate its cost.
	tr.MaxIdleConnsPerHost = 32
	tr.IdleConnTimeout = 90 * time.Second
	return &Submitter{
		endpoints: eps,
		client:    &http.Client{Timeout: cfg.Timeout, Transport: tr},
		log:       log,
	}, nil
}

// Tx submits one raw transaction and reports what the cluster did with it.
func (s *Submitter) Tx(ctx context.Context, raw []byte) (Outcome, error) {
	ep := s.endpoints[int(s.next.Add(1)-1)%len(s.endpoints)]

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep+"/tx", bytes.NewReader(raw))
	if err != nil {
		s.failed.Add(1)
		return Failed, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.client.Do(req)
	if err != nil {
		s.failed.Add(1)
		return Failed, fmt.Errorf("submit %s/tx: %w", ep, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	switch resp.StatusCode {
	case http.StatusOK:
		// The handler answers 200 for an already-known transaction too, so this
		// covers both first delivery and re-delivery.
		s.accepted.Add(1)
		return Accepted, nil

	case http.StatusConflict:
		// Spent / conflicting / locked: for a bridge these mean the cluster
		// already has this outpoint's spend — a duplicate in all but name.
		s.duplicate.Add(1)
		return Duplicate, nil

	case http.StatusBadRequest, http.StatusForbidden, http.StatusUnprocessableEntity:
		// Refused on merits (invalid, frozen, missing parent). Retrying the same
		// bytes cannot change the answer.
		s.rejected.Add(1)
		return Rejected, fmt.Errorf("rejected (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))

	default:
		s.failed.Add(1)
		return Failed, fmt.Errorf("submit %s/tx: http %d: %s", ep, resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

// Health reports whether at least one endpoint answers.
func (s *Submitter) Health(ctx context.Context) error {
	var lastErr error
	for _, ep := range s.endpoints {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep+"/health", nil)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("%s/health: http %d", ep, resp.StatusCode)
	}
	return lastErr
}

// Stats is a snapshot for logging and metrics.
type Stats struct{ Accepted, Duplicate, Rejected, Failed uint64 }

// Stats returns a snapshot of the submit outcome counters.
func (s *Submitter) Stats() Stats {
	return Stats{
		Accepted:  s.accepted.Load(),
		Duplicate: s.duplicate.Load(),
		Rejected:  s.rejected.Load(),
		Failed:    s.failed.Load(),
	}
}
