package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"

	"github.com/lightwebinc/teranode-bridge/cache"
	"github.com/lightwebinc/teranode-bridge/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/metrics"
	"github.com/lightwebinc/teranode-bridge/lanes"
	"github.com/lightwebinc/teranode-bridge/registry"
)

// stdTx is a minimal well-formed BRC-12 standard transaction: one input with an
// empty unlocking script, one output with an empty locking script.
func stdTx() []byte {
	var b bytes.Buffer
	b.Write([]byte{0x01, 0x00, 0x00, 0x00}) // version
	b.WriteByte(0x01)                       // input count
	b.Write(make([]byte, 36))               // prev txid + index
	b.WriteByte(0x00)                       // unlocking script length
	b.Write([]byte{0xff, 0xff, 0xff, 0xff}) // sequence
	b.WriteByte(0x01)                       // output count
	b.Write(make([]byte, 8))                // value
	b.WriteByte(0x00)                       // locking script length
	b.Write(make([]byte, 4))                // locktime
	return b.Bytes()
}

// efTx is the same transaction in BRC-30 extended format: marker after the
// version, and each input carries the spent value and locking script.
func efTx() []byte {
	var b bytes.Buffer
	b.Write([]byte{0x01, 0x00, 0x00, 0x00})             // version
	b.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}) // EF marker
	b.WriteByte(0x01)                                   // input count
	b.Write(make([]byte, 36))                           // prev txid + index
	b.WriteByte(0x00)                                   // unlocking script length
	b.Write([]byte{0xff, 0xff, 0xff, 0xff})             // sequence
	b.Write(make([]byte, 8))                            // spent value
	b.WriteByte(0x00)                                   // spent locking script length
	b.WriteByte(0x01)                                   // output count
	b.Write(make([]byte, 8))                            // value
	b.WriteByte(0x00)                                   // locking script length
	b.Write(make([]byte, 4))                            // locktime
	return b.Bytes()
}

func txFixtures(t *testing.T) (*cache.Generational, *registry.Registry) {
	t.Helper()
	return cache.NewGenerational(cache.Options{MaxBytes: 1 << 20, TTL: time.Minute}),
		registry.New(time.Minute, 1024)
}

// TestHandleTxRefusesStandardFormat pins the TX lane's admission rule: the lane
// carries BRC-30 EF only, so a BRC-12 standard transaction is refused before it
// can take a cache slot or a registry entry — the latter would suppress the EF
// copy of the same transaction as a duplicate.
func TestHandleTxRefusesStandardFormat(t *testing.T) {
	txs, seen := txFixtures(t)

	err := handleTx(context.Background(), stdTx(), txs, seen, nil)
	if !errors.Is(err, lanes.ErrReject) {
		t.Fatalf("standard tx must be refused with ErrReject, got %v", err)
	}
	if n := txs.Stats().Entries; n != 0 {
		t.Fatalf("refused tx must not be cached, got %d entries", n)
	}
	if n := seen.Stats().Entries; n != 0 {
		t.Fatalf("refused tx must not enter the registry, got %d entries", n)
	}
}

// TestHandleTxAcceptsEF is the other half: the same transaction in extended
// format is cached and registered.
func TestHandleTxAcceptsEF(t *testing.T) {
	txs, seen := txFixtures(t)

	if err := handleTx(context.Background(), efTx(), txs, seen, nil); err != nil {
		t.Fatalf("EF tx must be accepted: %v", err)
	}
	if n := txs.Stats().Entries; n != 1 {
		t.Fatalf("EF tx must be cached, got %d entries", n)
	}
	if n := seen.Stats().Entries; n != 1 {
		t.Fatalf("EF tx must enter the registry, got %d entries", n)
	}
}

// TestAnnounceFlagsRequirePeerID pins the startup refusal for announcing
// modes: an empty -peer-id is the exact config that circuit-breaks the
// cluster's catchup (the announce URL substitutes for the missing id), so it
// must never survive to a running announcer. Sink mode is validated elsewhere
// (the whole block is skipped).
func TestAnnounceFlagsRequirePeerID(t *testing.T) {
	if msg := announceFlagsErr(1, 1, "http://x", ""); msg == "" {
		t.Fatal("empty peer id must be refused for announcing modes")
	}
	if msg := announceFlagsErr(1, 1, "http://x", "12D3KooWExample"); msg != "" {
		t.Fatalf("valid announcing config refused: %q", msg)
	}
	if msg := announceFlagsErr(0, 1, "http://x", "12D3KooWExample"); msg == "" {
		t.Fatal("missing propagation endpoints must be refused")
	}
	if msg := announceFlagsErr(1, 1, "", "12D3KooWExample"); msg == "" {
		t.Fatal("missing advertise URL must be refused")
	}
}

// --- announce gate -----------------------------------------------------------
//
// The gate is the one thing `seen` must NOT decide on its own. `seen` records
// that a hash crossed the bridge — the reverse path's origin filter and the
// echo detector both read it — while `announced` records that the cluster was
// actually told. These tests pin all four rules, on both lanes: a first
// delivery announces, a redelivery after success does not, an object we
// ourselves submitted is never announced back at the cluster, and a redelivery
// after a FAILED announce announces again.

const testBaseURL = "http://[2001:db8::1]:9145/api/v1"

// stubAnnouncer stands in for *announce.Producer. err is what the next publish
// returns, so a test can fail an announce and then let it succeed.
type stubAnnouncer struct {
	err              error
	subtrees, blocks []string
}

func (s *stubAnnouncer) Subtree(_ context.Context, hash, _ string) error {
	s.subtrees = append(s.subtrees, hash)
	return s.err
}

func (s *stubAnnouncer) Block(_ context.Context, hash, _ string) error {
	s.blocks = append(s.blocks, hash)
	return s.err
}

// gateFixture is everything the two object handlers read and write.
type gateFixture struct {
	objects   *cache.Cache
	seen      *registry.Registry
	announced *registry.Registry
	rec       *metrics.Recorder
	log       *slog.Logger
}

func newGateFixture(t *testing.T) *gateFixture {
	t.Helper()
	return &gateFixture{
		objects:   cache.New(cache.Options{MaxBytes: 1 << 20, TTL: time.Minute}),
		seen:      registry.New(time.Minute, 1024),
		announced: registry.New(time.Minute, 1024),
		rec:       metrics.New(metrics.Options{}),
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// subtreeFrame is a minimal BRC-143 push frame: a 32-byte merkle root and a
// zero node count. handleSubtree reads the root and the node count, nothing
// else, but the frame is checked against the codec so the fixture stays honest.
func subtreeFrame(t *testing.T, fill byte) []byte {
	t.Helper()
	obj := append(bytes.Repeat([]byte{fill}, 32), make([]byte, 8)...)
	if n, err := objfmt.SubtreeSize(obj); err != nil || n != len(obj) {
		t.Fatalf("fixture is not a valid BRC-143 frame: size=%d err=%v len=%d", n, err, len(obj))
	}
	return obj
}

// blockFrame is a minimal but self-consistent BRC-144 push frame naming the
// given subtree roots. handleBlock hashes the header and walks the roots, so
// unlike the subtree fixture the prefix has to survive a real parse.
func blockFrame(t *testing.T, fill byte, roots ...[32]byte) []byte {
	t.Helper()
	var cb []byte
	cb = binary.LittleEndian.AppendUint32(cb, 1) // version
	cb = append(cb, 0x01)                        // input count
	cb = append(cb, make([]byte, 32)...)         // prev txid
	cb = binary.LittleEndian.AppendUint32(cb, 0xffffffff)
	cb = append(cb, 0x02, 0x51, 0x51) // script len + OP_1 OP_1
	cb = binary.LittleEndian.AppendUint32(cb, 0xffffffff)
	cb = append(cb, 0x01)                                 // output count
	cb = binary.LittleEndian.AppendUint64(cb, 5000000000) // satoshis
	cb = append(cb, 0x01, 0x51)                           // script len + OP_1
	cb = binary.LittleEndian.AppendUint32(cb, 0)          // locktime

	obj := make([]byte, 0, 256)
	obj = append(obj, bytes.Repeat([]byte{fill}, 80)...) // header: fill varies the block hash
	obj = binary.BigEndian.AppendUint64(obj, 1)          // transaction count
	obj = binary.BigEndian.AppendUint64(obj, uint64(len(cb)))
	obj = binary.BigEndian.AppendUint64(obj, uint64(len(roots))) // subtree count
	for _, r := range roots {
		obj = append(obj, r[:]...)
	}
	obj = append(obj, cb...)
	obj = binary.BigEndian.AppendUint64(obj, 800000) // height
	obj = binary.BigEndian.AppendUint64(obj, 0)      // coinbase BUMP length
	if n, err := objfmt.BlockSize(obj); err != nil || n != len(obj) {
		t.Fatalf("fixture is not a valid BRC-144 frame: size=%d err=%v len=%d", n, err, len(obj))
	}
	return obj
}

// laneCase drives one object class through the gate. The rules are identical on
// both lanes, so each is stated once and checked against both.
type laneCase struct {
	name    string
	frame   func(t *testing.T, fill byte) []byte
	id      func(t *testing.T, obj []byte) hashid.Hash
	deliver func(f *gateFixture, obj []byte, ann announcer) error
	sent    func(a *stubAnnouncer) []string
}

func laneCases() []laneCase {
	return []laneCase{
		{
			name:  "subtree",
			frame: func(t *testing.T, fill byte) []byte { return subtreeFrame(t, fill) },
			id: func(t *testing.T, obj []byte) hashid.Hash {
				t.Helper()
				h, err := hashid.FromWire(obj[:32])
				if err != nil {
					t.Fatalf("fixture root: %v", err)
				}
				return h
			},
			deliver: func(f *gateFixture, obj []byte, ann announcer) error {
				return handleSubtree(context.Background(), obj, f.objects, f.seen, f.announced,
					ann, testBaseURL, f.rec, f.log)
			},
			sent: func(a *stubAnnouncer) []string { return a.subtrees },
		},
		{
			name:  "block",
			frame: func(t *testing.T, fill byte) []byte { return blockFrame(t, fill) },
			id: func(t *testing.T, obj []byte) hashid.Hash {
				t.Helper()
				return hashid.DoubleSHA256(obj[:80])
			},
			deliver: func(f *gateFixture, obj []byte, ann announcer) error {
				return handleBlock(context.Background(), obj, f.objects, f.seen, f.announced,
					ann, testBaseURL, f.rec, f.log)
			},
			sent: func(a *stubAnnouncer) []string { return a.blocks },
		},
	}
}

// TestAnnounceGate is the defect and its guard rails in one sequence per lane.
// A failed announce must not poison the object's future: the bytes stay cached
// (the retrieval plane still has to serve a pull) but nothing is recorded, so
// the next redelivery announces again. A SUCCESSFUL announce still suppresses
// every redelivery after it, which is what stops a flapping link re-announcing
// the same object forever.
func TestAnnounceGate(t *testing.T) {
	boom := errors.New("kafka down")
	steps := []struct {
		name          string
		announceErr   error
		wantErr       bool
		wantAnnounces int // cumulative, after this delivery
	}{
		{"first delivery announces, and the announce fails", boom, true, 1},
		{"redelivery after a FAILED announce announces again", nil, false, 2},
		{"redelivery after a SUCCESSFUL announce is suppressed", nil, false, 2},
		{"and stays suppressed", nil, false, 2},
	}

	for _, lc := range laneCases() {
		t.Run(lc.name, func(t *testing.T) {
			f := newGateFixture(t)
			obj := lc.frame(t, 0x11)
			ann := &stubAnnouncer{}
			for _, st := range steps {
				ann.err = st.announceErr
				err := lc.deliver(f, obj, ann)
				switch {
				case st.wantErr && !errors.Is(err, boom):
					t.Fatalf("%s: failed announce must surface to the lane, got %v", st.name, err)
				case !st.wantErr && err != nil:
					t.Fatalf("%s: unexpected error %v", st.name, err)
				}
				if n := len(lc.sent(ann)); n != st.wantAnnounces {
					t.Fatalf("%s: want %d cumulative announces, got %d", st.name, st.wantAnnounces, n)
				}
				// Cached throughout, including while unannounced.
				if _, _, ok := f.objects.Get(cache.Key(lc.id(t, obj))); !ok {
					t.Fatalf("%s: object must stay cached and servable", st.name)
				}
			}
		})
	}
}

// TestAnnounceGateSuppressesOwnEcho pins the echo rule. Own-traffic exclusion
// does not cover the subtree and block classes, so everything this cluster
// pushes up comes back down the delivery lanes. The cluster produced it; it
// must never be announced back at the cluster as new — and unlike a failed
// announce, no redelivery changes that.
func TestAnnounceGateSuppressesOwnEcho(t *testing.T) {
	for _, lc := range laneCases() {
		t.Run(lc.name, func(t *testing.T) {
			f := newGateFixture(t)
			obj := lc.frame(t, 0x22)
			h := lc.id(t, obj)
			// Exactly what the reverse path leaves behind when it publishes.
			f.objects.Put(cache.Key(h), lc.name, obj)
			f.seen.Mark(registry.Key(h), registry.Submitted)

			ann := &stubAnnouncer{}
			for i := range 2 {
				if err := lc.deliver(f, obj, ann); err != nil {
					t.Fatalf("delivery %d: %v", i, err)
				}
				if n := len(lc.sent(ann)); n != 0 {
					t.Fatalf("delivery %d: our own echo must not be announced, got %d", i, n)
				}
			}
			// The direction must survive: Mark never downgrades, so the object
			// stays recognisable as ours for as long as the entry lives.
			if dir, known := f.seen.Lookup(registry.Key(h)); !known || dir != registry.Submitted {
				t.Fatalf("echo must stay registered as submitted, got dir=%v known=%v", dir, known)
			}
			if n := f.announced.Stats().Entries; n != 0 {
				t.Fatalf("an unannounced echo must not enter the announced registry, got %d", n)
			}
		})
	}
}

// TestDeliveryAlwaysRegistersSeen pins the origin filter's input. reverse.go's
// handle() treats any hash NOT in `seen` as locally originated and publishes it
// UP the tunnel; a fabric-delivered object that slipped out of `seen` would be
// republished into the fabric as ours. So every delivery must register, whatever
// the announce did — including the delivery whose announce failed, which is the
// one the new gate lets through a second time.
func TestDeliveryAlwaysRegistersSeen(t *testing.T) {
	cases := []struct {
		name        string
		announceErr error
		producer    bool
	}{
		{"announce succeeded", nil, true},
		{"announce failed", errors.New("kafka down"), true},
		{"sink mode, nothing announced", nil, false},
	}
	for _, lc := range laneCases() {
		for _, tc := range cases {
			t.Run(lc.name+"/"+tc.name, func(t *testing.T) {
				f := newGateFixture(t)
				obj := lc.frame(t, 0x33)
				h := lc.id(t, obj)
				var ann announcer
				if tc.producer {
					ann = &stubAnnouncer{err: tc.announceErr}
				}
				_ = lc.deliver(f, obj, ann)

				dir, known := f.seen.Lookup(registry.Key(h))
				if !known {
					t.Fatal("a delivered object must be in the seen registry, or the reverse path will publish it back up as ours")
				}
				if dir != registry.Delivered {
					t.Fatalf("want direction delivered, got %v", dir)
				}
			})
		}
	}
}

// TestBlockContainedSubtreeIsNotAnnouncedOnItsOwnLane pins the deliberate
// decision on the block pre-mark. handleBlock registers every subtree root the
// block names, so the reverse path cannot mistake another cluster's subtrees
// for this cluster's when gossip beats the fabric. That pre-mark ALSO suppresses
// the announce if the subtree then arrives on the subtree lane — the cluster
// learns of those subtrees from the block and fetches them while validating it —
// and splitting the registries must not change that, whether the block's own
// announce succeeded or failed.
func TestBlockContainedSubtreeIsNotAnnouncedOnItsOwnLane(t *testing.T) {
	for _, tc := range []struct {
		name        string
		announceErr error
	}{
		{"block announced", nil},
		{"block announce failed", errors.New("kafka down")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newGateFixture(t)
			sub := subtreeFrame(t, 0x44)
			var root [32]byte
			copy(root[:], sub[:32])
			blk := blockFrame(t, 0x55, root)
			ann := &stubAnnouncer{err: tc.announceErr}

			_ = handleBlock(context.Background(), blk, f.objects, f.seen, f.announced, ann, testBaseURL, f.rec, f.log)
			if len(ann.blocks) != 1 {
				t.Fatalf("the block itself must be announced once, got %d", len(ann.blocks))
			}

			ann.err = nil
			if err := handleSubtree(context.Background(), sub, f.objects, f.seen, f.announced,
				ann, testBaseURL, f.rec, f.log); err != nil {
				t.Fatalf("contained subtree delivery: %v", err)
			}
			if len(ann.subtrees) != 0 {
				t.Fatalf("a subtree the block already named must not be announced, got %d", len(ann.subtrees))
			}
			if _, known := f.seen.Lookup(registry.Key(hashid.Hash(root))); !known {
				t.Fatal("the contained root must stay in the seen registry for the origin filter")
			}
		})
	}
}

// TestSinkModeRecordsAndSuppresses pins sink behaviour: no announcer, so there
// is nothing to fail and the object counts as fully handled on first delivery.
func TestSinkModeRecordsAndSuppresses(t *testing.T) {
	for _, lc := range laneCases() {
		t.Run(lc.name, func(t *testing.T) {
			f := newGateFixture(t)
			obj := lc.frame(t, 0x66)
			for i := range 2 {
				if err := lc.deliver(f, obj, nil); err != nil {
					t.Fatalf("delivery %d: sink mode must not error: %v", i, err)
				}
			}
			if n := f.announced.Stats().Entries; n != 1 {
				t.Fatalf("sink mode must record the object exactly once, got %d entries", n)
			}
		})
	}
}

// TestDistinctObjectsEachAnnounce guards the obvious wrong fixes: suppressing
// nothing, or suppressing everything.
func TestDistinctObjectsEachAnnounce(t *testing.T) {
	for _, lc := range laneCases() {
		t.Run(lc.name, func(t *testing.T) {
			f := newGateFixture(t)
			ann := &stubAnnouncer{}
			for _, fill := range []byte{0x01, 0x02, 0x03} {
				if err := lc.deliver(f, lc.frame(t, fill), ann); err != nil {
					t.Fatalf("fill %#x: %v", fill, err)
				}
			}
			if n := len(lc.sent(ann)); n != 3 {
				t.Fatalf("three distinct objects must produce three announces, got %d", n)
			}
		})
	}
}
