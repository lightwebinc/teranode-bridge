// Package health implements the bridge's dependency-aggregating health surface,
// shaped to match Teranode's own (teranode/util/health + daemon/daemon.go).
//
// # The Teranode contract this mirrors
//
// A Teranode service exposes /health, /health/readiness and /health/liveness,
// accepts a ?timeout= override, and answers with a JSON body enumerating every
// dependency it has — each with its own status, message and error — rather than
// a bare 200. Liveness deliberately skips dependency probing: it asks only
// whether this process is alive, so a sick dependency never triggers a restart
// loop. That distinction is threaded through every check as `checkLiveness`.
//
// # Deliberate deviation: gating vs advisory dependencies
//
// Teranode's CheckAll fails the aggregate if ANY dependency fails. Applied
// literally to the bridge that is actively harmful: the retrieval plane serves
// objects out of a local cache, so a bridge with unreachable Kafka can still
// answer every pull for everything it has already announced. Failing readiness
// would pull it out of the retrieval Service and strand exactly those fetches —
// turning a degraded announce path into lost objects.
//
// So each check declares whether it gates readiness. The aggregate status is
// driven by the gating checks; every check, gating or not, appears in the body
// with its own status. `-health-strict` restores Teranode's all-or-nothing
// behaviour for deployments that want it.
//
// # Why /readyz is not this
//
// /readyz keeps its old, narrow meaning — every delivery lane is bound — because
// a standby bridge polls the primary's /readyz to decide whether to promote
// itself (see internal/reverse.RunPromoter). Folding dependency health into that
// signal would let a Kafka blip promote a standby while the primary is still
// publishing, and two submitters is a worse outcome than a late one.
package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// CheckFunc probes one dependency. It returns an HTTP status, a human-readable
// message, and an error if the probe itself failed — the same signature
// Teranode's util/health uses, so a check written for one is portable to the
// other.
type CheckFunc func(ctx context.Context, checkLiveness bool) (int, string, error)

// Check is one named dependency.
type Check struct {
	// Name is the resource name reported in the body.
	Name string
	// Check probes it.
	Check CheckFunc
	// Gating marks this dependency as one the bridge cannot serve its role
	// without. Non-gating dependencies are reported but do not fail the
	// aggregate unless strict mode is on.
	Gating bool
}

// Result is one dependency's outcome as it appears in the response body.
type Result struct {
	Resource string `json:"resource"`
	Status   string `json:"status"`
	Message  string `json:"message,omitempty"`
	Error    string `json:"error,omitempty"`
	Gating   bool   `json:"gating"`
}

// Report is the whole response body.
type Report struct {
	Status       string   `json:"status"`
	Dependencies []Result `json:"dependencies"`
}

// CheckAll runs every check and aggregates them.
//
// Field names match Teranode's body so the same tooling reads both; unlike
// Teranode's hand-built string, this is encoded by encoding/json, so a message
// containing a quote cannot produce a malformed document.
func CheckAll(ctx context.Context, checkLiveness, strict bool, checks []Check) (int, []byte) {
	overall := http.StatusOK
	results := make([]Result, 0, len(checks))

	for _, c := range checks {
		status, message, err := c.Check(ctx, checkLiveness)
		failed := err != nil || status != http.StatusOK
		if failed && (strict || c.Gating) {
			overall = http.StatusServiceUnavailable
		}
		r := Result{
			Resource: c.Name,
			Status:   strconv.Itoa(status),
			Message:  message,
			Gating:   c.Gating,
		}
		if err != nil {
			r.Error = err.Error()
		}
		results = append(results, r)
	}

	body, err := json.Marshal(Report{Status: strconv.Itoa(overall), Dependencies: results})
	if err != nil {
		// Marshalling a struct of strings cannot fail; if it somehow does, a
		// health endpoint must still answer something parseable.
		return http.StatusServiceUnavailable, []byte(`{"status":"503","dependencies":[]}`)
	}
	return overall, body
}

// DefaultTimeout bounds a health probe when the request does not override it,
// matching Teranode's daemon.
const DefaultTimeout = 5 * time.Second

// Handler serves one health route. liveness selects Teranode's liveness
// semantics (dependencies are skipped, not probed).
//
// The ?timeout= query parameter overrides the probe deadline, as Teranode's
// health handler allows, so an operator can distinguish "slow" from "down"
// without redeploying.
func Handler(base context.Context, liveness, strict bool, checks func() []Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		timeout := DefaultTimeout
		if v := r.URL.Query().Get("timeout"); v != "" {
			if parsed, err := time.ParseDuration(v); err == nil && parsed > 0 {
				timeout = parsed
			}
		}
		ctx, cancel := context.WithTimeout(base, timeout)
		defer cancel()

		status, body := CheckAll(ctx, liveness, strict, checks())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(append(body, '\n'))
	}
}

// Alive is the liveness answer for the process itself: if this code runs, the
// process runs. Teranode's liveness has the same shape — it is deliberately not
// a proxy for anything downstream.
func Alive() CheckFunc {
	return func(context.Context, bool) (int, string, error) {
		return http.StatusOK, "process is running", nil
	}
}

// Skip wraps a check so liveness returns OK without probing, which is what
// keeps a sick dependency from restarting a healthy process. Teranode does this
// per-check (see util/kafka.HealthChecker); doing it in one wrapper keeps every
// bridge check honest by construction.
func Skip(f CheckFunc) CheckFunc {
	return func(ctx context.Context, checkLiveness bool) (int, string, error) {
		if checkLiveness {
			return http.StatusOK, "liveness (dependency probe skipped)", nil
		}
		return f(ctx, checkLiveness)
	}
}

// Bool builds a check from a predicate, for state the bridge already knows and
// does not need to probe (lanes bound, submitter role held).
func Bool(ok func() bool, whenOK, whenNot string) CheckFunc {
	return func(context.Context, bool) (int, string, error) {
		if ok() {
			return http.StatusOK, whenOK, nil
		}
		return http.StatusServiceUnavailable, whenNot, nil
	}
}

// Ping builds a check from any function that returns an error, e.g. a Kafka
// client's Ping.
func Ping(name string, ping func(context.Context) error) CheckFunc {
	return Skip(func(ctx context.Context, _ bool) (int, string, error) {
		if err := ping(ctx); err != nil {
			return http.StatusServiceUnavailable, name + " unreachable", err
		}
		return http.StatusOK, name + " reachable", nil
	})
}

// HTTPGet builds a check that any of the given base URLs answers 2xx at path.
// Several endpoints are healthy if ONE answers: the tx pipeline round-robins
// across propagation endpoints and keeps working while a single one is down, so
// failing the check on the first bad endpoint would misreport a working bridge.
func HTTPGet(client *http.Client, bases []string, path string) CheckFunc {
	return Skip(func(ctx context.Context, _ bool) (int, string, error) {
		if len(bases) == 0 {
			return http.StatusOK, "not configured - skipping", nil
		}
		var lastErr error
		reached := 0
		for _, base := range bases {
			url := trimRight(base) + path
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				lastErr = err
				continue
			}
			resp, err := client.Do(req)
			if err != nil {
				lastErr = err
				continue
			}
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				reached++
				continue
			}
			lastErr = fmt.Errorf("%s: http %d", url, resp.StatusCode)
		}
		switch {
		case reached == len(bases):
			return http.StatusOK, fmt.Sprintf("all %d endpoints answering", reached), nil
		case reached > 0:
			// Degraded but working, so the STATUS is the verdict and the error
			// is folded into the message. Returning a non-nil error alongside a
			// 200 would make CheckAll count this as a failure — which under
			// -health-strict would fail readiness for a bridge that is
			// delivering perfectly well through its remaining endpoints.
			return http.StatusOK, fmt.Sprintf("%d of %d endpoints answering (%v)",
				reached, len(bases), lastErr), nil
		default:
			return http.StatusServiceUnavailable, "no endpoint answering", lastErr
		}
	})
}

func trimRight(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
