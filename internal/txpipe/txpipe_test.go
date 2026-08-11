package txpipe

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
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
// txid named in the error line is retried on /tx and eventually accepted,
// while the rest of the batch counts accepted immediately.
func TestPartialFailureRetry(t *testing.T) {
	var mu sync.Mutex
	var txsCalls, txCalls int
	var failDisplay string

	mux := http.NewServeMux()
	mux.HandleFunc("/txs", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		txsCalls++
		mu.Unlock()
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
		BatchTxs:  10, Linger: 5 * time.Millisecond, Inflight: 1, RetryAttempts: 3,
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
	if txCalls != 1 {
		t.Fatalf("retry /tx calls = %d, want 1", txCalls)
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
