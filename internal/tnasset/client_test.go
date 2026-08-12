package tnasset

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/teranode-bridge/internal/hashid"
)

// shortenLadder makes the rate-limit backoff test-fast and restores it after.
func shortenLadder(t *testing.T) {
	t.Helper()
	orig := rateLimitBackoff
	rateLimitBackoff = []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond}
	t.Cleanup(func() { rateLimitBackoff = orig })
}

// TestRetriesThrough429 pins the reason the ladder exists: the asset limiter
// trips on a burst of mined blocks, and without retry those objects are
// silently never published — indistinguishable downstream from "the miner
// produced nothing".
func TestRetriesThrough429(t *testing.T) {
	shortenLadder(t)
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 64)) // two 32-byte node hashes
	}))
	defer srv.Close()

	c := New(srv.URL, 5*time.Second)
	body, status, err := c.get(context.Background(), "/subtree/x")
	if err != nil || status != http.StatusOK {
		t.Fatalf("get = (%d, %v), want 200 after retries", status, err)
	}
	if len(body) != 64 {
		t.Fatalf("body %d bytes", len(body))
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3 (two 429s then success)", got)
	}
}

// TestGivesUpAfterLadder pins the bound: a permanently throttled endpoint must
// fail with a clear error rather than retry forever and wedge the reverse path.
func TestGivesUpAfterLadder(t *testing.T) {
	shortenLadder(t)
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := New(srv.URL, 5*time.Second)
	if _, _, err := c.get(context.Background(), "/subtree/x"); err == nil {
		t.Fatal("want an error after the ladder is exhausted")
	}
	if got, want := calls.Load(), int64(len(rateLimitBackoff)+1); got != want {
		t.Fatalf("calls = %d, want %d (one per rung plus the first try)", got, want)
	}
}

// TestNotFoundIsSkipNotError pins the reverse path's "not ours to publish
// yet" signal: a 404 must come back as ok=false with no error, so the caller
// skips rather than counting a failure.
func TestNotFoundIsSkipNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unknown", http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL, 5*time.Second)
	frame, ok, err := c.BuildSubtree(context.Background(), hashid.Hash{1})
	if err != nil || ok || frame != nil {
		t.Fatalf("BuildSubtree on 404 = (%v, %v, %v), want (nil, false, nil)", frame, ok, err)
	}
}

// TestContextCancelDuringBackoff pins that a shutdown mid-ladder returns
// promptly instead of sleeping out the remaining rungs.
func TestContextCancelDuringBackoff(t *testing.T) {
	orig := rateLimitBackoff
	rateLimitBackoff = []time.Duration{time.Hour}
	t.Cleanup(func() { rateLimitBackoff = orig })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()

	c := New(srv.URL, 5*time.Second)
	start := time.Now()
	if _, _, err := c.get(ctx, "/subtree/x"); err == nil {
		t.Fatal("want a cancellation error")
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("took %s — did not observe cancellation during backoff", el)
	}
}
