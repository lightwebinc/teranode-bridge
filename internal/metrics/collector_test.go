package metrics

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lightwebinc/teranode-bridge/internal/txpipe"
)

// TestGatherWithSourcesAttached pins that a scrape taken while the tx path is
// live returns every family that path owns.
//
// The families only materialise once a source is attached — a bridge with no
// tx lane scrapes clean whatever happens here — so this is the only place a
// missing or misspelled tx metric surfaces before an operator finds an empty
// panel.
func TestGatherWithSourcesAttached(t *testing.T) {
	r := New("test", "unit")

	p, err := txpipe.New(txpipe.Config{Endpoints: []string{"http://127.0.0.1:1"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("txpipe: %v", err)
	}
	r.SetSources(Sources{Tx: p})

	mfs, err := r.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	// Every family the tx path is responsible for must be present.
	got := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	for _, want := range []string{
		"btb_submit_total",
		"btb_txpipe_batches_total",
		"btb_txpipe_batch_seals_total",
		"btb_txpipe_retried_total",
		"btb_txpipe_retry_accepted_total",
		"btb_txpipe_unattributed_total",
		"btb_txpipe_rate_limited_total",
		"btb_txpipe_queue_depth",
	} {
		if !got[want] {
			t.Errorf("missing metric family %s", want)
		}
	}
}

// TestDescribeCoversCollect pins that Describe announces every descriptor
// Collect can emit.
//
// The registry tolerates the mismatch today — the gather above passes with
// descriptors missing — so this guards the contract rather than a live
// outage: Describe is what lets the registry detect a name or label collision
// at REGISTRATION time instead of at scrape time, and a descriptor it never
// heard about is exempt from that check. Every tx-pipe metric was undescribed
// when this test was written.
func TestDescribeCoversCollect(t *testing.T) {
	r := New("test", "unit")
	p, err := txpipe.New(txpipe.Config{Endpoints: []string{"http://127.0.0.1:1"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("txpipe: %v", err)
	}
	r.SetSources(Sources{Tx: p})
	c := &collector{rec: r}

	described := make(map[string]bool)
	dch := make(chan *prometheus.Desc, 256)
	go func() { c.Describe(dch); close(dch) }()
	for d := range dch {
		described[d.String()] = true
	}

	mch := make(chan prometheus.Metric, 256)
	go func() { c.Collect(mch); close(mch) }()
	for m := range mch {
		if d := m.Desc(); !described[d.String()] {
			t.Errorf("Collect emits an undescribed descriptor: %s", fqName(d))
		}
	}
}

// fqName pulls the metric name out of a Desc's String() form for a readable
// failure; the Desc type exposes no accessor.
func fqName(d *prometheus.Desc) string {
	s := d.String()
	const key = `fqName: "`
	i := strings.Index(s, key)
	if i < 0 {
		return s
	}
	rest := s[i+len(key):]
	if j := strings.Index(rest, `"`); j >= 0 {
		return rest[:j]
	}
	return s
}
