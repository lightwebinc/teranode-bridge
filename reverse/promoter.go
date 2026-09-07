package reverse

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// PromoterConfig tunes health-gated submitter failover.
type PromoterConfig struct {
	// ProbeURL is the PRIMARY bridge's /readyz. Configure this on the STANDBY
	// only: a configured primary never probes anyone and never auto-demotes.
	ProbeURL string
	// Interval between probes.
	Interval time.Duration
	// FailN consecutive failed probes promote the standby.
	FailN int
	// OKN consecutive healthy probes demote it again. Demotion is deliberately
	// slower than promotion: a flapping primary must not flap the role, and a
	// transient dual-publish is harmless (every receiver dedups by hash — the
	// failure model's "no corruption, just a gap" cuts both ways).
	OKN int
	// Probe overrides the HTTP check (tests). Nil uses a GET on ProbeURL.
	Probe func(ctx context.Context) bool
}

func (c *PromoterConfig) defaults() {
	if c.Interval <= 0 {
		c.Interval = 2 * time.Second
	}
	if c.FailN <= 0 {
		c.FailN = 5
	}
	if c.OKN <= 0 {
		c.OKN = 10
	}
}

// RunPromoter watches the primary and flips this subscriber's submitter role:
// FailN consecutive probe failures promote, OKN consecutive successes demote.
// It runs until ctx is cancelled. The worst case is bounded and benign in both
// directions — a dead primary costs FailN×Interval of publish gap (the failure
// model's existing "gap until promoted", now measured in seconds instead of an
// operator's pager), and a false promotion costs a window of dual-publish that
// hash dedup absorbs.
func (s *Subscriber) RunPromoter(ctx context.Context, cfg PromoterConfig, log *slog.Logger) {
	cfg.defaults()
	probe := cfg.Probe
	if probe == nil {
		client := &http.Client{Timeout: cfg.Interval}
		probe = func(ctx context.Context) bool {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.ProbeURL, nil)
			if err != nil {
				return false
			}
			resp, err := client.Do(req)
			if err != nil {
				return false
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
			_ = resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}
	}

	log.Info("submitter promoter watching primary", "probe", cfg.ProbeURL,
		"interval", cfg.Interval, "promote_after", cfg.FailN, "demote_after", cfg.OKN)

	var fails, oks int
	t := time.NewTicker(cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if probe(ctx) {
			fails, oks = 0, oks+1
			if oks >= cfg.OKN && s.Active() {
				s.SetActive(false)
				log.Info("primary healthy again; demoting to standby", "after_ok_probes", oks)
			}
			continue
		}
		oks, fails = 0, fails+1
		if fails >= cfg.FailN && !s.Active() {
			s.SetActive(true)
			log.Warn("primary unreachable; PROMOTED to submitter", "after_failed_probes", fails)
		}
	}
}
