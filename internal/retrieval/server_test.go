package retrieval

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"

	"github.com/lightwebinc/teranode-bridge/internal/cache"
	"github.com/lightwebinc/teranode-bridge/internal/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/tnwire"
)

// These tests pin the CLUSTER-FACING contract, not this package's internals.
// Every assertion here corresponds to something Teranode does with the response:
// a body shape it parses, a status it classifies, or a byte order it re-derives.
// Getting one wrong does not fail loudly at the bridge — it fails as a root-hash
// mismatch, an endless Kafka redelivery, or a subtree that can never be found.

func testServer(t *testing.T) (*Server, http.Handler, *cache.Cache, *cache.Cache) {
	t.Helper()
	objects := cache.New(cache.Options{})
	txs := cache.New(cache.Options{})
	s := New(Config{APIPrefix: "/api/v1"}, objects, objects, txs,
		slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	return s, s.Handler(), objects, txs
}

// subtreeFrame builds a BRC-143 frame: root ∥ u64 BE count ∥ nodes.
func subtreeFrame(root [32]byte, nodes [][32]byte) []byte {
	out := make([]byte, 0, objfmt.SubtreeHeaderSize+len(nodes)*32)
	out = append(out, root[:]...)
	out = binary.BigEndian.AppendUint64(out, uint64(len(nodes)))
	for _, n := range nodes {
		out = append(out, n[:]...)
	}
	return out
}

// minimalTx is a syntactically complete standard transaction.
func minimalTx(marker byte) []byte {
	var b []byte
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = append(b, 0x01)
	prev := make([]byte, 32)
	prev[0] = marker
	b = append(b, prev...)
	b = binary.LittleEndian.AppendUint32(b, 0)
	b = append(b, 0x01, 0x51)
	b = binary.LittleEndian.AppendUint32(b, 0xffffffff)
	b = append(b, 0x01)
	b = binary.LittleEndian.AppendUint64(b, 1000)
	b = append(b, 0x01, 0x51)
	b = binary.LittleEndian.AppendUint32(b, 0)
	return b
}

func blockFrame(t *testing.T, roots [][32]byte, height uint64) []byte {
	t.Helper()
	cb := minimalTx(0xC0)
	out := make([]byte, 0, 256)
	out = append(out, bytes.Repeat([]byte{0x7A}, 80)...)
	out = binary.BigEndian.AppendUint64(out, 7)   // tx count
	out = binary.BigEndian.AppendUint64(out, 999) // size
	out = binary.BigEndian.AppendUint64(out, uint64(len(roots)))
	for _, r := range roots {
		out = append(out, r[:]...)
	}
	out = append(out, cb...)
	out = binary.BigEndian.AppendUint64(out, height)
	out = binary.BigEndian.AppendUint64(out, 0) // no BUMP
	if n, err := objfmt.BlockSize(out); err != nil || n != len(out) {
		t.Fatalf("fixture is not a valid BRC-144 frame: n=%d err=%v len=%d", n, err, len(out))
	}
	return out
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	return rr
}

// A subtree is served as its bare node hashes — the frame minus its 40-byte
// header. Teranode rebuilds the tree from exactly these bytes and compares the
// root against the hash it announced, so any extra or missing byte surfaces as
// "subtree root hash mismatch" rather than as an error here.
func TestSubtreeServedAsBareNodeHashes(t *testing.T) {
	s, h, objects, _ := testServer(t)
	root := [32]byte{0xAA, 0x01}
	nodes := [][32]byte{{0x11}, {0x22}, {0x33}}
	frame := subtreeFrame(root, nodes)
	objects.Put(cache.Key(root), "subtree", frame)

	rr := get(t, h, "/api/v1/subtree/"+hashid.Hash(root).Display())
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	body := rr.Body.Bytes()
	if len(body) != len(nodes)*32 {
		t.Fatalf("body %d bytes, want %d (nodes only, no header)", len(body), len(nodes)*32)
	}
	if !bytes.Equal(body, frame[objfmt.SubtreeHeaderSize:]) {
		t.Fatal("body is not the frame's node section")
	}
	// The root must NOT appear: including the header is the obvious wrong answer.
	if bytes.HasPrefix(body, root[:]) {
		t.Fatal("response begins with the root — the 40-byte header was not stripped")
	}
	if s.Stats().Subtree != 1 {
		t.Fatalf("hit not counted: %+v", s.Stats())
	}
}

// The URL carries DISPLAY order; the cache is keyed by WIRE order. This is the
// one conversion that, if dropped, makes every object permanently unfindable
// while every unit in isolation still looks correct.
func TestSubtreeLookupConvertsDisplayOrderToWireOrder(t *testing.T) {
	_, h, objects, _ := testServer(t)
	var root [32]byte
	for i := range root {
		root[i] = byte(i + 1) // deliberately not a palindrome
	}
	objects.Put(cache.Key(root), "subtree", subtreeFrame(root, [][32]byte{{0x09}}))

	display := hashid.Hash(root).Display()
	if display == hex32(root) {
		t.Fatal("fixture is byte-symmetric; it cannot detect a missing reversal")
	}
	if rr := get(t, h, "/api/v1/subtree/"+display); rr.Code != http.StatusOK {
		t.Fatalf("display-order lookup failed: %d", rr.Code)
	}
	// The wire-order string must NOT resolve — that would mean no conversion.
	if rr := get(t, h, "/api/v1/subtree/"+hex32(root)); rr.Code == http.StatusOK {
		t.Fatal("wire-order hash also resolved: the display conversion is not happening")
	}
}

func hex32(b [32]byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 64)
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&0xf])
	}
	return string(out)
}

// An object we do not hold must be 404 — never 200 with an empty body, which
// blockvalidation rejects as "empty subtree received", and never 5xx, which it
// classifies as recoverable and retries forever.
func TestMissingObjectIsNotFoundNeverEmptyOKNever5xx(t *testing.T) {
	_, h, _, _ := testServer(t)
	absent := hashid.Hash([32]byte{0xDE, 0xAD}).Display()

	for _, path := range []string{
		"/api/v1/subtree/" + absent,
		"/api/v1/subtree_data/" + absent,
		"/api/v1/block/" + absent,
	} {
		rr := get(t, h, path)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s: status %d, want 404", path, rr.Code)
		}
		if rr.Code >= 500 {
			t.Fatalf("%s: 5xx on a missing object would loop the cluster's Kafka consumer", path)
		}
	}
}

// A block is served in Teranode's serialization: identical content, counts as
// CompactSize varints instead of fixed u64. The header must survive byte-exact
// or the block hash changes.
func TestBlockTranscodedToTeranodeSerialization(t *testing.T) {
	s, h, objects, _ := testServer(t)
	roots := [][32]byte{{0x11}, {0x22}}
	frame := blockFrame(t, roots, 800001)
	id := hashid.DoubleSHA256(frame[:80])
	objects.Put(cache.Key(id), "block", frame)

	rr := get(t, h, "/api/v1/block/"+id.Display())
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	body := rr.Body.Bytes()
	if !bytes.Equal(body[:80], frame[:80]) {
		t.Fatal("header not preserved byte-for-byte — the block hash would change")
	}
	if bytes.Equal(body, frame) {
		t.Fatal("body is the BRC-144 frame verbatim; it was never transcoded")
	}
	// The strongest check: parsing the response back must reproduce the frame.
	parsed, err := tnwire.FromTeranode(body)
	if err != nil {
		t.Fatalf("cluster-side parse of our response failed: %v", err)
	}
	back, err := parsed.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back, frame) {
		t.Fatal("round trip through the served body did not reproduce the frame")
	}
	if s.Stats().Block != 1 {
		t.Fatalf("hit not counted: %+v", s.Stats())
	}
}

// subtree_data is the member transactions in node order. The coinbase slot is a
// placeholder, not a transaction, and must be skipped — sending anything for it
// shifts every subsequent index and fails the cluster's per-index txid check.
func TestSubtreeDataSkipsCoinbasePlaceholderAndPreservesOrder(t *testing.T) {
	_, h, objects, txs := testServer(t)

	txA, txB := minimalTx(0xA1), minimalTx(0xB2)
	idA, err := objfmt.TxID(txA)
	if err != nil {
		t.Fatal(err)
	}
	idB, err := objfmt.TxID(txB)
	if err != nil {
		t.Fatal(err)
	}
	var placeholder [32]byte
	for i := range placeholder {
		placeholder[i] = 0xFF
	}
	root := [32]byte{0x5E}
	objects.Put(cache.Key(root), "subtree", subtreeFrame(root, [][32]byte{placeholder, idA, idB}))
	txs.Put(cache.Key(idA), "tx", txA)
	txs.Put(cache.Key(idB), "tx", txB)

	rr := get(t, h, "/api/v1/subtree_data/"+hashid.Hash(root).Display())
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%q", rr.Code, rr.Body.String())
	}
	want := append(append([]byte{}, txA...), txB...)
	if !bytes.Equal(rr.Body.Bytes(), want) {
		t.Fatal("body is not the two member transactions in node order")
	}
	if bytes.Contains(rr.Body.Bytes(), placeholder[:]) {
		t.Fatal("the coinbase placeholder was emitted as if it were a transaction")
	}
}

// One missing member means the whole response is useless to the cluster (it
// checks the count), so the honest answer is 404 — which sends it to the batch
// route — not a short body that fails a confusing way.
func TestSubtreeDataMissingMemberIs404NotPartial(t *testing.T) {
	_, h, objects, txs := testServer(t)
	txA := minimalTx(0xA1)
	idA, _ := objfmt.TxID(txA)
	root := [32]byte{0x77}
	objects.Put(cache.Key(root), "subtree", subtreeFrame(root, [][32]byte{idA, {0x99}}))
	txs.Put(cache.Key(idA), "tx", txA) // second member deliberately absent

	rr := get(t, h, "/api/v1/subtree_data/"+hashid.Hash(root).Display())
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rr.Code)
	}
	if bytes.Contains(rr.Body.Bytes(), txA) {
		t.Fatal("a partial body was emitted; the cluster would fail on a count mismatch")
	}
}

// Extended-format transactions are converted to standard serialization, which is
// what Teranode's own asset serves on these routes. The txid is unchanged, so
// this is invisible to the cluster except that it matches its expectations.
func TestSubtreeDataServesStandardSerializationForExtendedFormat(t *testing.T) {
	_, h, objects, txs := testServer(t)
	std := minimalTx(0xEE)
	ef := toEF(t, std)
	if !objfmt.IsEF(ef) {
		t.Fatal("fixture is not extended format")
	}
	id, err := objfmt.TxID(ef)
	if err != nil {
		t.Fatal(err)
	}
	root := [32]byte{0x3C}
	objects.Put(cache.Key(root), "subtree", subtreeFrame(root, [][32]byte{id}))
	txs.Put(cache.Key(id), "tx", ef)

	rr := get(t, h, "/api/v1/subtree_data/"+hashid.Hash(root).Display())
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	if objfmt.IsEF(rr.Body.Bytes()) {
		t.Fatal("served the extended form; Teranode's asset serves standard bytes here")
	}
	if got, err := objfmt.TxID(rr.Body.Bytes()); err != nil || got != id {
		t.Fatalf("txid changed in conversion: %v", err)
	}
}

// toEF wraps a standard transaction in the BRC-30 marker with empty prevout
// data — enough for IsEF and the codec, which is all this package touches.
func toEF(t *testing.T, std []byte) []byte {
	t.Helper()
	out := make([]byte, 0, len(std)+16)
	out = append(out, std[:4]...)                         // version
	out = append(out, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF) // EF marker
	rest := std[4:]
	inCount := rest[0]
	out = append(out, inCount)
	off := 1
	for i := byte(0); i < inCount; i++ {
		out = append(out, rest[off:off+36]...) // prevout
		off += 36
		sl := int(rest[off])
		out = append(out, rest[off:off+1+sl+4]...) // script + sequence
		off += 1 + sl + 4
		out = binary.LittleEndian.AppendUint64(out, 0) // prev satoshis
		out = append(out, 0x00)                        // prev script len
	}
	out = append(out, rest[off:]...)
	if n, err := objfmt.TxSize(out); err != nil || n != len(out) {
		t.Fatalf("hand-built EF is not a whole transaction: n=%d err=%v len=%d", n, err, len(out))
	}
	return out
}

// The batch route takes raw 32-byte txids with no count or delimiter.
func TestTxsBatchRoute(t *testing.T) {
	s, h, _, txs := testServer(t)
	txA, txB := minimalTx(0x01), minimalTx(0x02)
	idA, _ := objfmt.TxID(txA)
	idB, _ := objfmt.TxID(txB)
	txs.Put(cache.Key(idA), "tx", txA)
	txs.Put(cache.Key(idB), "tx", txB)

	body := append(append([]byte{}, idA[:]...), idB[:]...)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/api/v1/subtree/"+hashid.Hash([32]byte{0x01}).Display()+"/txs", bytes.NewReader(body)))
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	if !bytes.Equal(rr.Body.Bytes(), append(append([]byte{}, txA...), txB...)) {
		t.Fatal("response is not the requested transactions concatenated")
	}
	if s.Stats().Txs != 1 {
		t.Fatalf("hit not counted: %+v", s.Stats())
	}
}

func TestTxsBatchRejectsMalformedAndMissing(t *testing.T) {
	_, h, _, txs := testServer(t)
	tx := minimalTx(0x03)
	id, _ := objfmt.TxID(tx)
	txs.Put(cache.Key(id), "tx", tx)
	path := "/api/v1/subtree/" + hashid.Hash([32]byte{0x01}).Display() + "/txs"

	// Not a whole number of txids: a request we cannot even interpret.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(make([]byte, 33))))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("malformed body: status %d, want 400", rr.Code)
	}

	// One known, one unknown: the count would not match, so 404 not a short body.
	body := append(append([]byte{}, id[:]...), bytes.Repeat([]byte{0x5A}, 32)...)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing member: status %d, want 404", rr.Code)
	}
	if bytes.Contains(rr.Body.Bytes(), tx) {
		t.Fatal("emitted a partial batch")
	}
}

func TestBadHashIsBadRequest(t *testing.T) {
	_, h, _, _ := testServer(t)
	for _, bad := range []string{"nothex", strings.Repeat("ab", 31), strings.Repeat("zz", 32)} {
		if rr := get(t, h, "/api/v1/subtree/"+bad); rr.Code != http.StatusBadRequest {
			t.Fatalf("%q: status %d, want 400", bad, rr.Code)
		}
	}
}

// Routes the bridge has no data for must 404 rather than error: catchup paths
// are how a cluster probes a peer, and a 5xx there reads as a fault.
func TestUnservedRoutesAre404(t *testing.T) {
	_, h, _, _ := testServer(t)
	hash := hashid.Hash([32]byte{0x01}).Display()
	for _, path := range []string{
		"/api/v1/headers_from_common_ancestor/" + hash,
		"/api/v1/blocks/" + hash + "?n=10",
		"/api/v1/tx/" + hash,
		"/", "/api/v1/", "/some/other/thing",
	} {
		if rr := get(t, h, path); rr.Code != http.StatusNotFound {
			t.Fatalf("%s: status %d, want 404", path, rr.Code)
		}
	}
}

// One cluster caller appends the API prefix to an already-prefixed base URL.
// Serving the alias turns a confusing "missing object" into a normal fetch.
func TestDoublePrefixAliasIsServed(t *testing.T) {
	_, h, objects, txs := testServer(t)
	tx := minimalTx(0x44)
	id, _ := objfmt.TxID(tx)
	root := [32]byte{0x6B}
	objects.Put(cache.Key(root), "subtree", subtreeFrame(root, [][32]byte{id}))
	txs.Put(cache.Key(id), "tx", tx)

	display := hashid.Hash(root).Display()
	plain := get(t, h, "/api/v1/subtree_data/"+display)
	alias := get(t, h, "/api/v1/api/v1/subtree_data/"+display)
	if plain.Code != http.StatusOK || alias.Code != http.StatusOK {
		t.Fatalf("plain=%d alias=%d", plain.Code, alias.Code)
	}
	if !bytes.Equal(plain.Body.Bytes(), alias.Body.Bytes()) {
		t.Fatal("alias served different bytes")
	}
}

// A cached frame that cannot be served is a genuine fault (500), and must be
// distinguishable from "we do not have it" (404) — they mean different things to
// the cluster and to whoever is debugging.
func TestCorruptCachedFrameIsServerErrorNot404(t *testing.T) {
	s, h, objects, _ := testServer(t)
	root := [32]byte{0x8D}
	objects.Put(cache.Key(root), "subtree", []byte{0x01, 0x02}) // shorter than the header
	if rr := get(t, h, "/api/v1/subtree/"+hashid.Hash(root).Display()); rr.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", rr.Code)
	}

	id := hashid.Hash([32]byte{0x9E})
	objects.Put(cache.Key(id), "block", bytes.Repeat([]byte{0xFF}, 120)) // not a block
	if rr := get(t, h, "/api/v1/block/"+id.Display()); rr.Code != http.StatusInternalServerError {
		t.Fatalf("block: status %d, want 500", rr.Code)
	}
	if st := s.Stats(); st.Errors != 2 {
		t.Fatalf("errors=%d, want 2 (%+v)", st.Errors, st)
	}
}

// The announced URL is what the cluster concatenates paths onto. A trailing
// slash yields "//subtree/", and a missing prefix yields a 404 for everything.
func TestBaseURLIsConcatenationSafe(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := cache.New(cache.Options{})
	for _, tc := range []struct{ prefix, advertise, want string }{
		{"/api/v1", "http://10.0.0.1:9145", "http://10.0.0.1:9145/api/v1"},
		{"/api/v1", "http://10.0.0.1:9145/", "http://10.0.0.1:9145/api/v1"},
		{"api/v1", "http://10.0.0.1:9145", "http://10.0.0.1:9145/api/v1"},
		{"", "http://10.0.0.1:9145", "http://10.0.0.1:9145/api/v1"},
	} {
		got := New(Config{APIPrefix: tc.prefix}, c, c, c, log).BaseURL(tc.advertise)
		if got != tc.want {
			t.Fatalf("prefix=%q advertise=%q -> %q, want %q", tc.prefix, tc.advertise, got, tc.want)
		}
		if strings.Contains(strings.TrimPrefix(got, "http://"), "//") {
			t.Fatalf("%q would produce a doubled slash when a path is appended", got)
		}
	}
}

func TestMissesAreCounted(t *testing.T) {
	s, h, _, _ := testServer(t)
	absent := hashid.Hash([32]byte{0x01, 0x02}).Display()
	get(t, h, "/api/v1/subtree/"+absent)
	get(t, h, "/api/v1/block/"+absent)
	if st := s.Stats(); st.Miss != 2 {
		t.Fatalf("miss=%d, want 2 (%+v)", st.Miss, st)
	}
}

// Serve is what production runs: it binds, wires the same routes, and shuts down
// on context cancel. Testing only Handler would leave the wiring — and the
// shutdown path a redeploy depends on — unexercised.
func TestServeBindsAndShutsDown(t *testing.T) {
	objects := cache.New(cache.Options{})
	root := [32]byte{0xF0}
	nodes := [][32]byte{{0x01}, {0x02}}
	objects.Put(cache.Key(root), "subtree", subtreeFrame(root, nodes))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	s := New(Config{Listen: addr, APIPrefix: "/api/v1"}, objects, objects, objects,
		slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()

	var resp *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err = http.Get("http://" + addr + "/api/v1/subtree/" + hashid.Hash(root).Display())
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		cancel()
		t.Fatalf("server never accepted a request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(body) != len(nodes)*32 {
		t.Fatalf("status=%d body=%d bytes", resp.StatusCode, len(body))
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil on cancel", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancel — a redeploy would hang")
	}
}

// A bind failure must be reported, not swallowed: a bridge that silently serves
// nothing looks identical to one the cluster cannot reach.
func TestServeReportsBindFailure(t *testing.T) {
	c := cache.New(cache.Options{})
	s := New(Config{Listen: "256.256.256.256:1", APIPrefix: "/api/v1"}, c, c, c,
		slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	if err := s.Serve(context.Background()); err == nil {
		t.Fatal("expected an error from an unbindable address")
	}
}
