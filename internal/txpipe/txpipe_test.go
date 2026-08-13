package txpipe

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"

	"github.com/lightwebinc/teranode-bridge/internal/hashid"
)

// rawTx builds a minimal valid BRC-12 transaction: one input spending prev:0,
// one 1-byte-script output. Fixed 62-byte size keeps server-side splitting
// trivial.
func rawTx(prev [32]byte, nonce byte) []byte {
	b := make([]byte, 0, 62)
	b = append(b, 1, 0, 0, 0)             // version
	b = append(b, 1)                      // input count
	b = append(b, prev[:]...)             // prev txid (wire order)
	b = append(b, 0, 0, 0, 0)             // vout
	b = append(b, 1, nonce)               // unlocking script len + script
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF) // sequence
	b = append(b, 1)                      // output count
	b = append(b, 1, 0, 0, 0, 0, 0, 0, 0) // value
	b = append(b, 1, 0x51)                // locking script len + OP_TRUE
	b = append(b, 0, 0, 0, 0)             // locktime
	return b
}

func txidOf(t *testing.T, raw []byte) hashid.Hash {
	t.Helper()
	id, err := objfmt.TxID(raw)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	return hashid.Hash(id)
}

// splitBody chops a /txs body back into 62-byte transactions.
func splitBody(t *testing.T, body []byte) [][]byte {
	t.Helper()
	if len(body)%62 != 0 {
		t.Fatalf("body %d bytes is not a whole number of test txs", len(body))
	}
	var out [][]byte
	for i := 0; i < len(body); i += 62 {
		out = append(out, body[i:i+62])
	}
	return out
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached in time")
}

// TestDependencySealing pins the /txs caller contract: a parent and its child
// must never share a request. The child arriving while its parent's batch is
// open must seal that batch and open a new one.
func TestDependencySealing(t *testing.T) {
	var mu sync.Mutex
	var requests [][][]byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 0, 4096)
		buf := make([]byte, 1024)
		for {
			n, err := r.Body.Read(buf)
			body = append(body, buf[:n]...)
			if err != nil {
				break
			}
		}
		mu.Lock()
		requests = append(requests, splitBody(t, body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	// One builder: dependency sealing is a per-builder property (multi-builder
	// routing already separates txs into distinct requests), so exercising it
	// requires parent and child to reach the same builder.
	p, err := New(Config{
		Endpoints: []string{srv.URL},
		BatchTxs:  100, Linger: 500 * time.Millisecond, Inflight: 2, Builders: 1,
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()

	parent := rawTx([32]byte{0xAA}, 1)
	parentID := txidOf(t, parent)
	child := rawTx([32]byte(parentID), 2) // spends the parent

	if err := p.Enqueue(ctx, parent, parentID); err != nil {
		t.Fatal(err)
	}
	if err := p.Enqueue(ctx, child, txidOf(t, child)); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return p.Stats().Accepted == 2 })
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("want 2 requests (parent sealed by dependency), got %d", len(requests))
	}
	for _, req := range requests {
		ids := map[hashid.Hash]bool{}
		for _, tx := range req {
			ids[txidOf(t, tx)] = true
		}
		if ids[parentID] && ids[txidOf(t, child)] {
			t.Fatal("parent and child shared a request — /txs contract violated")
		}
	}
	if s := p.Stats(); s.SealDep != 1 {
		t.Fatalf("SealDep = %d, want 1", s.SealDep)
	}
}

// TestPartialFailureRetry pins the 500-body re-attribution path: the failed
// txid named in the error line is retried as its own /txs BATCH and eventually
// accepted, while the rest of the original batch counts accepted immediately.
//
// The mock rejects the named transaction once and accepts it afterwards, which
// is what really happens — a missing parent lands in a different request while
// the retry ladder waits. It also asserts the singleton /tx endpoint is never
// touched: retry cost must scale with batches, not with transactions.
func TestPartialFailureRetry(t *testing.T) {
	var mu sync.Mutex
	var txsCalls, txCalls int
	var failDisplay string

	mux := http.NewServeMux()
	// Faithful mock: propagation only names transactions that were actually IN
	// this request. A mock that names a foreign txid every time would exercise
	// the unattributed path instead (see TestUnattributedErrorLine).
	mux.HandleFunc("/txs", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		txsCalls++
		mu.Unlock()
		present := false
		for _, tx := range splitBody(t, body) {
			if txidOf(t, tx).Display() == failDisplay {
				present = true
			}
		}
		mu.Lock()
		first := txsCalls == 1
		mu.Unlock()
		if !present || !first {
			// Second time around the parent has landed, so it is accepted.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, "Failed to process transactions:\n[ProcessTransaction][%s] missing parent\n", failDisplay)
	})
	mux.HandleFunc("/tx", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		txCalls++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	p, err := New(Config{
		Endpoints: []string{srv.URL},
		// Builders: 1 so both transactions land in ONE batch; the default
		// sharding would route them to different builders and the count of
		// /txs requests would stop meaning what this test asserts.
		BatchTxs: 10, Linger: 5 * time.Millisecond, Inflight: 1, Builders: 1, RetryAttempts: 3,
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()

	a := rawTx([32]byte{1}, 1)
	b := rawTx([32]byte{2}, 2)
	bID := txidOf(t, b)
	failDisplay = bID.Display()

	if err := p.Enqueue(ctx, a, txidOf(t, a)); err != nil {
		t.Fatal(err)
	}
	if err := p.Enqueue(ctx, b, bID); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		s := p.Stats()
		return s.Accepted == 2 && s.RetryAccepted == 1
	})
	cancel()
	<-done

	s := p.Stats()
	if s.Retried != 1 || s.Rejected != 0 || s.Failed != 0 {
		t.Fatalf("stats = %+v, want exactly one clean retry", s)
	}
	mu.Lock()
	defer mu.Unlock()
	if txCalls != 0 {
		t.Fatalf("singleton /tx calls = %d, want 0 — a retry is a batch", txCalls)
	}
	if txsCalls != 2 {
		t.Fatalf("/txs requests = %d, want 2 (the batch, then one retry batch)", txsCalls)
	}
}

// TestInputRefsEF pins the EF walk: prevout txids must be extracted from a
// BRC-30 transaction with its per-input extensions.
func TestInputRefsEF(t *testing.T) {
	prev := [32]byte{0x42}
	b := make([]byte, 0, 96)
	b = append(b, 1, 0, 0, 0)             // version
	b = append(b, 0, 0, 0, 0, 0, 0xEF)    // EF marker
	b = append(b, 1)                      // input count
	b = append(b, prev[:]...)             // prev txid
	b = append(b, 0, 0, 0, 0)             // vout
	b = append(b, 1, 0x00)                // unlocking script
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF) // sequence
	b = append(b, 8, 0, 0, 0, 0, 0, 0, 0) // spent value (8 bytes LE)
	b = append(b, 1, 0x51)                // spent locking script
	b = append(b, 1)                      // output count
	b = append(b, 1, 0, 0, 0, 0, 0, 0, 0) // value
	b = append(b, 1, 0x51)                // locking script
	b = append(b, 0, 0, 0, 0)             // locktime

	var got []hashid.Hash
	if err := inputRefs(b, func(h hashid.Hash) { got = append(got, h) }); err != nil {
		t.Fatalf("inputRefs: %v", err)
	}
	if len(got) != 1 || got[0] != hashid.Hash(prev) {
		t.Fatalf("got %v, want the EF input's prevout", got)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestUnattributedErrorLine pins the accounting guard: an error line naming a
// txid that is not a batch member means the batch's outcome is partly
// UNKNOWN. Those transactions must not be counted accepted — silently
// overstating delivery is the failure mode this counter exists to prevent.
func TestUnattributedErrorLine(t *testing.T) {
	foreign := strings.Repeat("ab", 32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, "Failed to process transactions:\n[ProcessTransaction][%s] unknown\n", foreign)
	}))
	defer srv.Close()

	p, err := New(Config{
		Endpoints: []string{srv.URL},
		BatchTxs:  10, Linger: 5 * time.Millisecond, Inflight: 1, Builders: 1, RetryAttempts: 0,
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()

	a := rawTx([32]byte{9}, 9)
	if err := p.Enqueue(ctx, a, txidOf(t, a)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return p.Stats().Unattributed == 1 })
	if s := p.Stats(); s.Accepted != 0 {
		t.Fatalf("accepted = %d, want 0 — an unknown outcome must never count as delivered", s.Accepted)
	}
}

// TestWholeBatchFailureSalvages pins the salvage path: when every endpoint
// refuses the whole batch, the members are retried as ONE further batch rather
// than written off — a batch-shaped fault does not make its members
// unacceptable — and the singleton /tx endpoint is never used, because a
// per-member fan-out is what collapses a real cluster.
func TestWholeBatchFailureSalvages(t *testing.T) {
	var txCalls atomic.Int64
	mux := http.NewServeMux()
	var txsCalls atomic.Int64
	mux.HandleFunc("/txs", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		// The first submission (and its one failover attempt) is refused
		// batch-shaped; the salvage batch that follows is accepted.
		if txsCalls.Add(1) <= 2 {
			http.Error(w, "Invalid request body: too much data", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/tx", func(w http.ResponseWriter, _ *http.Request) {
		txCalls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	p, err := New(Config{
		Endpoints: []string{srv.URL},
		BatchTxs:  10, Linger: 5 * time.Millisecond, Inflight: 1, Builders: 1, RetryAttempts: 2,
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()

	for i := 0; i < 3; i++ {
		tx := rawTx([32]byte{byte(20 + i)}, byte(i))
		if err := p.Enqueue(ctx, tx, txidOf(t, tx)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return p.Stats().Accepted == 3 })
	if got := txCalls.Load(); got != 0 {
		t.Fatalf("singleton /tx submits = %d, want 0 — salvage must be one batch", got)
	}
	if got := txsCalls.Load(); got != 3 {
		t.Fatalf("/txs requests = %d, want 3 (submit + failover + one salvage batch)", got)
	}
	if s := p.Stats(); s.Failed != 0 {
		t.Fatalf("failed = %d, want 0 — members were salvageable", s.Failed)
	}
}

// TestDuplicateInputLineAttributesToSubject pins the fix for a real
// accounting corruption: propagation's duplicate-input error quotes TWO
// 64-hex hashes — the subject transaction and the prevout it double-spends —
// and matching every hex token in the body booked the prevout as a phantom
// unattributed failure, subtracting a genuine acceptance for every such line.
// Only the bracketed subject names a transaction the batch is answering for.
func TestDuplicateInputLineAttributesToSubject(t *testing.T) {
	a := rawTx([32]byte{21}, 21)
	b := rawTx([32]byte{22}, 22)
	idA := txidOf(t, a)
	prevout := strings.Repeat("cd", 32) // a hash that is NOT a batch member

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		// Verbatim shape of Server.go's duplicate-input rejection.
		_, _ = fmt.Fprintf(w, "Failed to process transactions:\nTX_INVALID (69): [ProcessTransaction][%s] duplicate input found: %s:0\n",
			idA.Display(), prevout)
	}))
	defer srv.Close()

	p, err := New(Config{
		Endpoints: []string{srv.URL},
		BatchTxs:  10, Linger: 5 * time.Millisecond, Inflight: 1, Builders: 1, RetryAttempts: 0,
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()

	for _, raw := range [][]byte{a, b} {
		if err := p.Enqueue(ctx, raw, txidOf(t, raw)); err != nil {
			t.Fatal(err)
		}
	}
	// One member named, one silent: exactly one rejected and one accepted.
	waitFor(t, func() bool { return p.Stats().Rejected == 1 })
	s := p.Stats()
	if s.Unattributed != 0 {
		t.Errorf("unattributed = %d, want 0 — the prevout hash is not a failed transaction", s.Unattributed)
	}
	if s.Accepted != 1 {
		t.Errorf("accepted = %d, want 1 — the unnamed member was processed", s.Accepted)
	}
}

// TestBatchTxsClampedBelowServerCap pins the off-by-one in propagation's
// batch guard. Server.go checks `totalNrTransactions >= maxTransactionsPerRequest`
// at the TOP of its read loop, so a batch of exactly 1024 is read, processed
// and published in full, and only then answered 400. Batching at the limit
// would make every member land AND be resubmitted as a duplicate, so the pipe
// must clamp strictly below it.
func TestBatchTxsClampedBelowServerCap(t *testing.T) {
	for _, req := range []int{1024, 5000} {
		c := Config{Endpoints: []string{"http://127.0.0.1:1"}, BatchTxs: req}
		c.defaults()
		if c.BatchTxs != 1023 {
			t.Errorf("BatchTxs %d clamped to %d, want 1023", req, c.BatchTxs)
		}
	}
	// A request below the cap is left alone.
	c := Config{Endpoints: []string{"http://127.0.0.1:1"}, BatchTxs: 256}
	c.defaults()
	if c.BatchTxs != 256 {
		t.Errorf("BatchTxs = %d, want 256 untouched", c.BatchTxs)
	}
}

// TestRateLimitRetriesWholeBatch pins that a 429 never fans out. Propagation
// rate-limits per source IP per endpoint, so splitting a refused batch into
// per-member requests would multiply the request rate by the batch size
// against the limiter that just refused it. The batch must be retried whole.
func TestRateLimitRetriesWholeBatch(t *testing.T) {
	orig := rateLimitBackoff
	rateLimitBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { rateLimitBackoff = orig })

	var batchCalls, singleCalls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/txs", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		// Throttle the first two attempts, then accept the whole batch.
		if batchCalls.Add(1) <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/tx", func(w http.ResponseWriter, r *http.Request) {
		singleCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	p, err := New(Config{
		Endpoints: []string{srv.URL},
		BatchTxs:  10, Linger: 5 * time.Millisecond, Inflight: 1, Builders: 1, RetryAttempts: 3,
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()

	for i := 0; i < 4; i++ {
		raw := rawTx([32]byte{31}, byte(i))
		if err := p.Enqueue(ctx, raw, txidOf(t, raw)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return p.Stats().Accepted == 4 })
	if got := singleCalls.Load(); got != 0 {
		t.Errorf("/tx called %d times — a throttled batch must not fan out into per-member requests", got)
	}
	if s := p.Stats(); s.RateLimited != 2 {
		t.Errorf("RateLimited = %d, want 2", s.RateLimited)
	}
}
