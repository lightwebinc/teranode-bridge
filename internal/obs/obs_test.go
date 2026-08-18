package obs

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// sampleCount reads a labelled histogram's observation count.
func sampleCount(t *testing.T, h *prometheus.HistogramVec, labels ...string) uint64 {
	t.Helper()
	m := &dto.Metric{}
	o, err := h.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("label values: %v", err)
	}
	if err := o.(prometheus.Metric).Write(m); err != nil {
		t.Fatalf("write: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestFirstPullObservesOnce pins the announce→pull SLI: the metric is about the
// cluster's REACTION time, so a second pull of the same object must not be
// counted as another reaction.
func TestFirstPullObservesOnce(t *testing.T) {
	f := NewFirstPull(16, time.Minute)
	before := sampleCount(t, AnnounceToFirstPull, "subtree")

	f.Announced("aa", "subtree")
	if f.Len() != 1 {
		t.Fatalf("awaiting count %d, want 1", f.Len())
	}
	f.Pulled("aa")
	f.Pulled("aa")

	if got := sampleCount(t, AnnounceToFirstPull, "subtree") - before; got != 1 {
		t.Fatalf("observed %d times, want exactly 1", got)
	}
	if f.Len() != 0 {
		t.Fatalf("entry not released after the first pull: %d", f.Len())
	}
}

// TestFirstPullIgnoresUnknown pins that a pull we never announced — an object
// the cluster asks for because a peer told it, or one announced before restart
// — is silently ignored rather than observed with a nonsense duration.
func TestFirstPullIgnoresUnknown(t *testing.T) {
	f := NewFirstPull(16, time.Minute)
	before := sampleCount(t, AnnounceToFirstPull, "block")
	f.Pulled("never-announced")
	if got := sampleCount(t, AnnounceToFirstPull, "block") - before; got != 0 {
		t.Fatalf("observed %d for an unannounced pull, want 0", got)
	}
}

// TestFirstPullBounded pins the memory bound. An object that is never pulled
// would otherwise leak an entry forever, and the tracker must lose observations
// rather than lose the process.
func TestFirstPullBounded(t *testing.T) {
	f := NewFirstPull(4, time.Hour)
	for i := range 100 {
		f.Announced(string(rune('a'+i%26))+string(rune('a'+i/26)), "subtree")
	}
	if f.Len() > 4 {
		t.Fatalf("tracker grew past its cap: %d entries, max 4", f.Len())
	}
}

// TestFirstPullExpires pins the TTL sweep, using an injected clock so the test
// does not sleep.
func TestFirstPullExpires(t *testing.T) {
	f := NewFirstPull(16, time.Minute)
	now := time.Unix(1_700_000_000, 0)
	f.nowFunc = func() time.Time { return now }

	f.Announced("aa", "subtree")
	if f.Len() != 1 {
		t.Fatalf("entry not recorded")
	}

	// Past the TTL, and past the once-a-second gc throttle.
	now = now.Add(2 * time.Minute)
	f.Announced("bb", "subtree")
	if f.Len() != 1 {
		t.Fatalf("expired entry survived the sweep: %d entries", f.Len())
	}
}

// TestNilFirstPullIsSafe pins that a bridge built without the tracker — sink
// mode has no announce side — never dereferences it.
func TestNilFirstPullIsSafe(t *testing.T) {
	var f *FirstPull
	f.Announced("aa", "subtree")
	f.Pulled("aa")
	if f.Len() != 0 {
		t.Fatal("nil tracker reported entries")
	}
}

// TestSetFSMStateIsExclusive pins that the info gauge never shows two states at
// once, which is what would happen if the previous state's series were left
// behind on a transition.
func TestSetFSMStateIsExclusive(t *testing.T) {
	SetFSMState("CATCHINGBLOCKS", 2)
	SetFSMState("RUNNING", 1)

	for _, s := range FSMStates {
		g, err := ClusterFSMStateInfo.GetMetricWithLabelValues(s)
		if err != nil {
			t.Fatalf("label %s: %v", s, err)
		}
		m := &dto.Metric{}
		if err := g.Write(m); err != nil {
			t.Fatalf("write: %v", err)
		}
		want := 0.0
		if s == "RUNNING" {
			want = 1
		}
		if got := m.GetGauge().GetValue(); got != want {
			t.Errorf("state %s = %v, want %v", s, got, want)
		}
	}
}

// TestPresetCreatesSeriesAtZero pins the property the whole freshness scheme
// rests on: `time() - metric > N` matches NOTHING when the series is absent, so
// a bridge that never received an object would look healthy to the alert
// written to catch exactly that.
func TestPresetCreatesSeriesAtZero(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	Preset(start, []string{"tx", "subtree", "block"}, true)

	reg := prometheus.NewRegistry()
	reg.MustRegister(LastObjectTime, LastNotificationTime)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	seen := map[string]int{}
	for _, mf := range mfs {
		seen[mf.GetName()] = len(mf.GetMetric())
	}
	if seen["teranode_bridge_last_object_timestamp_seconds"] < 3 {
		t.Errorf("lane freshness series not preset: %v", seen)
	}
	if seen["teranode_bridge_last_notification_timestamp_seconds"] < 1 {
		t.Errorf("notification freshness series not preset: %v", seen)
	}

	// Seeded with the START time, not zero. `time() - 0` is fifty-odd years, so
	// a zero seed would fire every staleness alert the instant the process
	// started — the opposite of the behaviour the alert was written for.
	g, err := LastObjectTime.GetMetricWithLabelValues("tx")
	if err != nil {
		t.Fatalf("label values: %v", err)
	}
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := m.GetGauge().GetValue(); got != float64(start.Unix()) {
		t.Errorf("freshness seeded with %v, want the process start time %v", got, start.Unix())
	}
}
