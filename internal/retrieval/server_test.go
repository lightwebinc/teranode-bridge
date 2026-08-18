package retrieval

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
	dto "github.com/prometheus/client_model/go"

	"github.com/lightwebinc/teranode-bridge/internal/cache"
	"github.com/lightwebinc/teranode-bridge/internal/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/obs"
	"time"
)

// The rules under test are the two that keep the cluster healthy:
//
//   - never 200 with an empty or wrong body (a wrong subtree body fails a
//     root-hash check as silent corruption),
//   - never 5xx for something simply not held (on the block path a 5xx is
//     classified recoverable and the Kafka message redelivers forever).

func testServer(t *testing.T) (*Server, *cache.Cache, *cache.Generational, *httptest.Server) {
	t.Helper()
	objects := cache.New(cache.Options{})
	txs := cache.NewGenerational(cache.Options{})
	s := New(Config{APIPrefix: "/api/v1"}, objects, objects, txs,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := httptest.NewServer(s.Handler())
	t.Cleanup(h.Close)
	return s, objects, txs, h
}

// rawTx is a minimal valid BRC-12 transaction (62 bytes): version | 1 input
// (32B prev, vout, 1-byte script, seq) | 1 output (value, OP_TRUE) | locktime.
func rawTx(nonce byte) []byte {
	b := make([]byte, 62)
	b[0], b[4] = 1, 1                                   // version, input count
	b[41], b[42] = 1, nonce                             // unlocking script
	b[43], b[44], b[45], b[46] = 0xFF, 0xFF, 0xFF, 0xFF // sequence
	b[47], b[48] = 1, 1                                 // output count, value=1
	b[56], b[57] = 1, 0x51                              // locking script len, OP_TRUE
	return b
}

func mustTxID(t *testing.T, raw []byte) [32]byte {
	t.Helper()
	id, err := objfmt.TxID(raw)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	return id
}

// subtreeFrame builds a BRC-143 frame: root | u64 count | nodes.
func subtreeFrame(root [32]byte, nodes ...[32]byte) []byte {
	out := append([]byte{}, root[:]...)
	out = binary.BigEndian.AppendUint64(out, uint64(len(nodes)))
	for _, n := range nodes {
		out = append(out, n[:]...)
	}
	return out
}

func get(t *testing.T, h *httptest.Server, path string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(h.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// TestUnknownIs404Never5xx pins the redelivery-loop guard on every route.
func TestUnknownIs404Never5xx(t *testing.T) {
	_, _, _, h := testServer(t)
	unknown := strings.Repeat("ab", 32)
	for _, path := range []string{
		"/api/v1/subtree/" + unknown,
		"/api/v1/subtree_data/" + unknown,
		"/api/v1/block/" + unknown,
		"/api/v1/api/v1/subtree_data/" + unknown, // doubled-prefix alias
		"/api/v1/blocks",                         // catchup route we do not serve
		"/tx",
	} {
		code, _ := get(t, h, path)
		if code != http.StatusNotFound {
			t.Errorf("%s = %d, want 404", path, code)
		}
	}
}

// TestBadHashIs400 pins that an unparseable hash is the caller's fault.
func TestBadHashIs400(t *testing.T) {
	_, _, _, h := testServer(t)
	code, _ := get(t, h, "/api/v1/subtree/nothex")
	if code != http.StatusBadRequest {
		t.Fatalf("bad hash = %d, want 400", code)
	}
}

// TestGetSubtreeIsFrameMinusHeader pins the no-transformation contract: the
// response is exactly the BRC-143 frame with its 40-byte header removed.
func TestGetSubtreeIsFrameMinusHeader(t *testing.T) {
	_, objects, _, h := testServer(t)
	root := [32]byte{0xAA}
	n1, n2 := [32]byte{1}, [32]byte{2}
	frame := subtreeFrame(root, n1, n2)
	objects.Put(cache.Key(root), "subtree", frame)

	code, body := get(t, h, "/api/v1/subtree/"+hashid.Hash(root).Display())
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if !bytes.Equal(body, frame[objfmt.SubtreeHeaderSize:]) {
		t.Fatal("body is not frame-minus-header")
	}
	if len(body) == 0 {
		t.Fatal("200 with empty body")
	}
}

// TestSubtreeDataSkipsPlaceholderAndConverts pins member serving: the 0xFF×32
// coinbase placeholder is skipped, and an EF member is served in standard
// serialization (matching the cluster's own asset behaviour).
func TestSubtreeDataSkipsPlaceholderAndConverts(t *testing.T) {
	_, objects, txs, h := testServer(t)

	member := rawTx(7)
	memberID, err := objfmt.TxID(member)
	if err != nil {
		t.Fatal(err)
	}
	txs.Put(cache.Key(memberID), "tx", member)

	var placeholder [32]byte
	for i := range placeholder {
		placeholder[i] = 0xFF
	}
	root := [32]byte{0xBB}
	frame := subtreeFrame(root, placeholder, memberID)
	objects.Put(cache.Key(root), "subtree", frame)

	code, body := get(t, h, "/api/v1/subtree_data/"+hashid.Hash(root).Display())
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if !bytes.Equal(body, member) {
		t.Fatalf("want just the member tx (placeholder skipped), got %d bytes", len(body))
	}
}

// TestSubtreeDataMissingMemberIs404 pins the clean-404 fallback: a partial
// body fails the cluster's per-index check anyway, so a missing member must
// 404 the whole request.
func TestSubtreeDataMissingMemberIs404(t *testing.T) {
	_, objects, _, h := testServer(t)
	root := [32]byte{0xCC}
	missing := [32]byte{0xDD}
	objects.Put(cache.Key(root), "subtree", subtreeFrame(root, missing))

	code, _ := get(t, h, "/api/v1/subtree_data/"+hashid.Hash(root).Display())
	if code != http.StatusNotFound {
		t.Fatalf("missing member = %d, want 404", code)
	}
}

// TestBatchTxs pins the batch route: raw 32-byte txids in, matching
// transactions concatenated out; count must match exactly (404 on any miss);
// non-multiple-of-32 body is a 400.
func TestBatchTxs(t *testing.T) {
	_, _, txs, h := testServer(t)
	a, b := rawTx(1), rawTx(2)
	aID := mustTxID(t, a)
	bID := mustTxID(t, b)
	txs.Put(cache.Key(aID), "tx", a)
	txs.Put(cache.Key(bID), "tx", b)

	post := func(body []byte) (int, []byte) {
		resp, err := http.Post(h.URL+"/api/v1/subtree/"+strings.Repeat("00", 32)+"/txs",
			"application/octet-stream", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		rb, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, rb
	}

	code, body := post(append(aID[:], bID[:]...))
	if code != http.StatusOK || !bytes.Equal(body, append(append([]byte{}, a...), b...)) {
		t.Fatalf("batch = %d, %d bytes", code, len(body))
	}

	var unknown [32]byte
	unknown[0] = 0xEE
	if code, _ := post(append(aID[:], unknown[:]...)); code != http.StatusNotFound {
		t.Fatalf("batch with missing member = %d, want 404", code)
	}
	if code, _ := post([]byte{1, 2, 3}); code != http.StatusBadRequest {
		t.Fatalf("ragged body = %d, want 400", code)
	}
}

// TestBaseURL pins the announce-URL join: no trailing slash, prefix appended.
func TestBaseURL(t *testing.T) {
	s, _, _, _ := testServer(t)
	if got := s.BaseURL("http://[fd00::1]:9145/"); got != "http://[fd00::1]:9145/api/v1" {
		t.Fatalf("BaseURL = %q", got)
	}
}

// TestNotFoundClassifiesChainSyncRoutes pins the canary: a chain-sync route
// request must count under UnservedChainSync — it is the signal that the
// cluster selected the bridge as a catchup source, which the synthetic
// peer-id divert exists to prevent.
func TestNotFoundClassifiesChainSyncRoutes(t *testing.T) {
	s, _, _, srv := testServer(t)
	defer srv.Close()
	h := s.Handler()

	for _, path := range []string{
		"/api/v1/headers_from_common_ancestor/0000000000000000000000000000000000000000000000000000000000000000?n=1000",
		"/api/v1/blocks?n=100",
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404 — a chain route must fail loudly, not pretend", path, rec.Code)
		}
	}
	st := s.Stats()
	if st.UnservedChainSync != 2 {
		t.Fatalf("UnservedChainSync = %d, want 2", st.UnservedChainSync)
	}

	// An unknown non-chain route counts as unserved but NOT as chain_sync.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/frobnicate", nil))
	if st := s.Stats(); st.UnservedChainSync != 2 || st.UnservedRoute != 3 {
		t.Fatalf("after non-chain route: chain=%d route=%d, want 2/3", st.UnservedChainSync, st.UnservedRoute)
	}
}

// TestFirstPullClosesTheLoop pins the bridge's own end-to-end SLI end to end:
// the announce side starts the stopwatch, a real pull through the real route
// table stops it, and only the FIRST pull counts.
//
// This is the one measurement neither the fabric nor the cluster makes — the
// cluster does not know when it was told, and the fabric does not know when the
// cluster acted — so if the wiring between the two halves breaks, nothing else
// fails to notice.
func TestFirstPullClosesTheLoop(t *testing.T) {
	s, objects, _, h := testServer(t)
	f := obs.NewFirstPull(16, time.Minute)
	s.SetFirstPull(f)

	root := [32]byte{9}
	frame := make([]byte, objfmt.SubtreeHeaderSize+32)
	copy(frame, root[:])
	objects.Put(cache.Key(root), "subtree", frame)

	hash := hashid.Hash(root)
	f.Announced(hash.Display(), "subtree")
	if f.Len() != 1 {
		t.Fatalf("announcement not tracked: %d", f.Len())
	}

	for i := range 2 {
		resp, err := h.Client().Get(h.URL + "/api/v1/subtree/" + hash.Display())
		if err != nil {
			t.Fatalf("pull %d: %v", i, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("pull %d: status %d", i, resp.StatusCode)
		}
	}

	if f.Len() != 0 {
		t.Fatalf("pull did not close the announce loop: %d still awaiting", f.Len())
	}
}

// TestListeningGatesReadiness pins the predicate the health endpoint gates on.
// It must be false before Serve binds: an announcement already sent points at
// this socket, and Teranode does not retry a failed subtree fetch.
func TestListeningGatesReadiness(t *testing.T) {
	s, _, _, _ := testServer(t)
	if s.Listening() {
		t.Fatal("reports listening before Serve bound a socket")
	}
}

// TestUnservedRouteDoesNotStampLastPull pins the meaning of the last-pull
// freshness gauge: "the cluster is still fetching objects from us".
//
// The catch-all is timed but must not stamp it. A cluster that had stopped
// pulling entirely while still occasionally probing an unserved route — a
// chain-sync attempt, a stray /tx — would otherwise keep the gauge fresh, and
// the staleness alert built on it would never fire.
func TestUnservedRouteDoesNotStampLastPull(t *testing.T) {
	_, _, _, h := testServer(t)

	read := func() float64 {
		t.Helper()
		g, err := obs.LastPullTime.GetMetricWithLabelValues()
		if err != nil {
			t.Fatalf("gauge: %v", err)
		}
		m := &dto.Metric{}
		if err := g.Write(m); err != nil {
			t.Fatalf("write: %v", err)
		}
		return m.GetGauge().GetValue()
	}

	obs.LastPullTime.WithLabelValues().Set(0)
	resp, err := h.Client().Get(h.URL + "/headers_from_common_ancestor")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unserved route answered %d, want 404", resp.StatusCode)
	}
	if got := read(); got != 0 {
		t.Fatalf("an unserved route stamped last-pull (%v); a cluster that stopped "+
			"pulling but kept probing would look fresh forever", got)
	}

	// A route the bridge actually serves must stamp it, 404 or not — the
	// cluster asked us for an object, which is the thing being measured.
	obs.LastPullTime.WithLabelValues().Set(0)
	resp, err = h.Client().Get(h.URL + "/api/v1/subtree/" + hashid.Hash([32]byte{1}).Display())
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	_ = resp.Body.Close()
	if read() == 0 {
		t.Fatal("a served route did not stamp last-pull")
	}
}
