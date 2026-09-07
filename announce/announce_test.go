package announce

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// The announcement message is hand-encoded rather than generated, so these
// tests are the only thing standing between a field-number typo and a cluster
// that silently never pulls: a message the consumer cannot parse produces no
// error anywhere on our side (Kafka consumers swallow and commit), it just
// means the object is never fetched.

// TestEncodeFieldNumbersAndOrder pins the wire contract byte-for-byte:
// field 1 = hash, 2 = URL, 3 = peer_id, ascending, length-delimited.
func TestEncodeFieldNumbersAndOrder(t *testing.T) {
	m := Message{Hash: "aa", URL: "http://x/api/v1", PeerID: "p"}
	got := m.Encode()

	var want []byte
	want = protowire.AppendTag(want, 1, protowire.BytesType)
	want = protowire.AppendString(want, "aa")
	want = protowire.AppendTag(want, 2, protowire.BytesType)
	want = protowire.AppendString(want, "http://x/api/v1")
	want = protowire.AppendTag(want, 3, protowire.BytesType)
	want = protowire.AppendString(want, "p")

	if !bytes.Equal(got, want) {
		t.Fatalf("wire bytes differ\n got %x\nwant %x", got, want)
	}
}

// TestEmptyPeerIDIsOmitted pins the canonical encoding AND the operational
// rule behind it: peer_id must be ABSENT, not present-and-empty. An identity
// the cluster's p2p service does not recognise gets block-path fetches
// refused, so the field is left off the wire entirely.
func TestEmptyPeerIDIsOmitted(t *testing.T) {
	got := Message{Hash: "aa", URL: "u"}.Encode()
	if bytes.Contains(got, []byte{0x1a}) { // tag for field 3, bytes type
		t.Fatalf("field 3 present with empty PeerID: %x", got)
	}
	m, err := Decode(got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m.PeerID != "" {
		t.Fatalf("PeerID = %q, want empty", m.PeerID)
	}
}

// TestRoundTrip pins Decode against Encode across the shapes the bridge
// actually produces.
func TestRoundTrip(t *testing.T) {
	for _, m := range []Message{
		{Hash: "deadbeef", URL: "http://[fd00::1]:9145/api/v1"},
		{Hash: "aa", URL: "u", PeerID: "12D3KooWfoo"},
		{}, // degenerate: everything omitted
	} {
		out, err := Decode(m.Encode())
		if err != nil {
			t.Fatalf("decode %+v: %v", m, err)
		}
		if out != m {
			t.Fatalf("round trip: got %+v want %+v", out, m)
		}
	}
}

// TestDecodeRejectsGarbage pins that Decode reports malformed input rather
// than returning a half-filled message a caller might trust.
func TestDecodeRejectsGarbage(t *testing.T) {
	for name, b := range map[string][]byte{
		"truncated length": {0x0a, 0x05, 'a'},
		"bad tag":          {0xff, 0xff, 0xff},
	} {
		if _, err := Decode(b); err == nil {
			t.Fatalf("%s: want error, got nil", name)
		}
	}
}
