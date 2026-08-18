package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func okCheck() CheckFunc {
	return func(context.Context, bool) (int, string, error) {
		return http.StatusOK, "fine", nil
	}
}

func badCheck() CheckFunc {
	return func(context.Context, bool) (int, string, error) {
		return http.StatusServiceUnavailable, "down", errors.New("boom")
	}
}

// TestAdvisoryFailureDoesNotGate is the whole reason this package deviates from
// Teranode's CheckAll. A shared dependency is seen failing by EVERY bridge in
// front of a cluster at the same instant; if that failed readiness, all of them
// would leave the retrieval Service together and strand the pulls for objects
// they had already announced — turning a degraded announce path into lost
// objects.
func TestAdvisoryFailureDoesNotGate(t *testing.T) {
	status, body := CheckAll(context.Background(), false, false, []Check{
		{Name: "Lanes", Check: okCheck(), Gating: true},
		{Name: "Kafka", Check: badCheck()},
	})
	if status != http.StatusOK {
		t.Fatalf("advisory failure gated readiness: got %d, want 200", status)
	}

	var rep Report
	if err := json.Unmarshal(body, &rep); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if len(rep.Dependencies) != 2 {
		t.Fatalf("want 2 dependencies, got %d", len(rep.Dependencies))
	}
	// Reported, not hidden: the operator still learns Kafka is down.
	var kafka *Result
	for i := range rep.Dependencies {
		if rep.Dependencies[i].Resource == "Kafka" {
			kafka = &rep.Dependencies[i]
		}
	}
	if kafka == nil {
		t.Fatal("advisory dependency missing from the body")
	}
	if kafka.Status != "503" || kafka.Error == "" {
		t.Errorf("advisory failure not reported: %+v", kafka)
	}
	if kafka.Gating {
		t.Error("Kafka reported as gating")
	}
}

func TestGatingFailureFailsReadiness(t *testing.T) {
	status, _ := CheckAll(context.Background(), false, false, []Check{
		{Name: "Lanes", Check: badCheck(), Gating: true},
		{Name: "Kafka", Check: okCheck()},
	})
	if status != http.StatusServiceUnavailable {
		t.Fatalf("gating failure did not fail readiness: got %d", status)
	}
}

// TestStrictCollapsesTheDistinction pins that -health-strict really does restore
// Teranode's all-or-nothing behaviour, since that is the escape hatch offered
// to deployments that would rather fail closed.
func TestStrictCollapsesTheDistinction(t *testing.T) {
	status, _ := CheckAll(context.Background(), false, true, []Check{
		{Name: "Lanes", Check: okCheck(), Gating: true},
		{Name: "Kafka", Check: badCheck()},
	})
	if status != http.StatusServiceUnavailable {
		t.Fatalf("strict mode ignored an advisory failure: got %d", status)
	}
}

// TestLivenessSkipsDependencies pins Teranode's liveness semantics: a sick
// dependency must never restart a healthy process.
func TestLivenessSkipsDependencies(t *testing.T) {
	probed := false
	wrapped := Skip(func(context.Context, bool) (int, string, error) {
		probed = true
		return http.StatusServiceUnavailable, "down", errors.New("boom")
	})
	status, _ := CheckAll(context.Background(), true, true, []Check{
		{Name: "Kafka", Check: wrapped, Gating: true},
	})
	if probed {
		t.Error("liveness probed a dependency")
	}
	if status != http.StatusOK {
		t.Fatalf("liveness failed on a dependency: got %d", status)
	}
}

// TestBodyEscapesQuotes guards the one concrete defect in the upstream shape
// this package copies: Teranode builds the body with fmt.Sprintf, so a message
// containing a quote produces a document no parser accepts.
func TestBodyEscapesQuotes(t *testing.T) {
	_, body := CheckAll(context.Background(), false, false, []Check{
		{Name: `we"ird`, Check: func(context.Context, bool) (int, string, error) {
			return http.StatusOK, `message with " and \ in it`, nil
		}},
	})
	var rep Report
	if err := json.Unmarshal(body, &rep); err != nil {
		t.Fatalf("quote in a message produced invalid JSON: %v\n%s", err, body)
	}
}

func TestHandlerTimeoutOverride(t *testing.T) {
	var got time.Duration
	checks := func() []Check {
		return []Check{{Name: "slow", Check: func(ctx context.Context, _ bool) (int, string, error) {
			dl, ok := ctx.Deadline()
			if ok {
				got = time.Until(dl).Round(time.Second)
			}
			return http.StatusOK, "ok", nil
		}}}
	}
	h := Handler(context.Background(), false, false, checks)

	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodGet, "/health?timeout=30s", nil))
	if got != 30*time.Second {
		t.Fatalf("?timeout= ignored: deadline in %v, want 30s", got)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type %q, want application/json", ct)
	}
}

// TestHTTPGetPartialReach pins that one dead propagation endpoint out of
// several is not an outage: the tx pipeline round-robins and keeps working, so
// reporting the bridge unhealthy would be wrong.
func TestHTTPGetPartialReach(t *testing.T) {
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer live.Close()

	status, msg, _ := HTTPGet(live.Client(), []string{live.URL, "http://127.0.0.1:1"}, "/health")(
		context.Background(), false)
	if status != http.StatusOK {
		t.Fatalf("partial reach reported unhealthy: %d %s", status, msg)
	}

	status, _, _ = HTTPGet(live.Client(), []string{"http://127.0.0.1:1"}, "/health")(
		context.Background(), false)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("no endpoint reachable reported healthy: %d", status)
	}
}

// TestPartialReachIsNotAFailureUnderStrict pins the interaction between
// HTTPGet's degraded-but-working verdict and CheckAll's failure rule. CheckAll
// treats a non-nil error as a failure regardless of status, so returning both a
// 200 and an error would fail readiness under -health-strict for a bridge that
// is delivering perfectly well through its remaining endpoints.
func TestPartialReachIsNotAFailureUnderStrict(t *testing.T) {
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer live.Close()

	status, body := CheckAll(context.Background(), false, true, []Check{
		{Name: "Propagation", Check: HTTPGet(live.Client(), []string{live.URL, "http://127.0.0.1:1"}, "/health")},
	})
	if status != http.StatusOK {
		t.Fatalf("partial reach failed readiness under strict: %d\n%s", status, body)
	}

	var rep Report
	if err := json.Unmarshal(body, &rep); err != nil {
		t.Fatalf("body: %v", err)
	}
	// The detail must survive: degraded-but-working still has to name the
	// endpoint that is down.
	if !strings.Contains(rep.Dependencies[0].Message, "1 of 2") {
		t.Errorf("degraded detail lost from the message: %q", rep.Dependencies[0].Message)
	}
}
