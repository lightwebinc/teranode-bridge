package reverse

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// TestPromoterLifecycle pins the failover state machine: FailN consecutive
// failures promote, OKN consecutive successes demote, and a flapping primary
// (alternating results) moves the role in neither direction.
func TestPromoterLifecycle(t *testing.T) {
	s := &Subscriber{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	var healthy atomic.Bool
	healthy.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		s.RunPromoter(ctx, PromoterConfig{
			Interval: 2 * time.Millisecond, FailN: 3, OKN: 5,
			Probe: func(context.Context) bool { return healthy.Load() },
		}, s.log)
		close(done)
	}()

	wait := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timeout: %s", what)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}

	// Healthy primary: standby stays inactive.
	time.Sleep(20 * time.Millisecond)
	if s.Active() {
		t.Fatal("standby promoted while primary healthy")
	}

	// Primary dies: promotion after FailN.
	healthy.Store(false)
	wait("promotion", s.Active)

	// Primary returns: demotion after OKN.
	healthy.Store(true)
	wait("demotion", func() bool { return !s.Active() })

	// Flap: alternate every probe — neither threshold accumulates.
	var n atomic.Int64
	flap := func(context.Context) bool { return n.Add(1)%2 == 0 }
	cancel()
	<-done
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go s.RunPromoter(ctx2, PromoterConfig{
		Interval: time.Millisecond, FailN: 3, OKN: 5, Probe: flap,
	}, s.log)
	time.Sleep(50 * time.Millisecond)
	if s.Active() {
		t.Fatal("flapping primary flapped the role")
	}
}
