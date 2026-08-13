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

	"github.com/lightwebinc/teranode-bridge/internal/cache"
	"github.com/lightwebinc/teranode-bridge/internal/hashid"
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
