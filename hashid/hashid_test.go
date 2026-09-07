package hashid

import (
	"encoding/hex"
	"testing"
)

// The genesis block header and its well-known hash: the canonical check that
// wire order and display order are the reverse of each other, and that we get
// the direction right rather than merely self-consistent.
func TestDoubleSHA256AndDisplayAgainstGenesis(t *testing.T) {
	header, err := hex.DecodeString(
		"0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd" +
			"7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c")
	if err != nil {
		t.Fatal(err)
	}
	const genesisDisplay = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"

	h := DoubleSHA256(header)
	if got := h.Display(); got != genesisDisplay {
		t.Fatalf("genesis hash display form:\n got %s\nwant %s", got, genesisDisplay)
	}
	// Internal order is the byte reversal, so the wire form must NOT equal the
	// display hex — the bug this package exists to prevent.
	if hex.EncodeToString(h.Wire()) == genesisDisplay {
		t.Fatal("wire order equals display order; the reversal is missing")
	}
}

func TestParseDisplayRoundTrip(t *testing.T) {
	const display = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
	h, err := ParseDisplay(display)
	if err != nil {
		t.Fatal(err)
	}
	if h.Display() != display {
		t.Fatalf("round trip: %s", h.Display())
	}
	// A URL path hash must resolve to the same cache key as the object we stored
	// from the wire, or every pull would miss.
	fromWire, err := FromWire(h.Wire())
	if err != nil {
		t.Fatal(err)
	}
	if fromWire != h {
		t.Fatal("wire round trip changed the key")
	}
}

func TestParseDisplayRejectsBadInput(t *testing.T) {
	for _, s := range []string{"", "abcd", "zz" + "00000000000000000000000000000000000000000000000000000000000000"} {
		if _, err := ParseDisplay(s); err == nil {
			t.Fatalf("expected error for %q", s)
		}
	}
}

func TestFromWireRejectsShort(t *testing.T) {
	if _, err := FromWire(make([]byte, 31)); err == nil {
		t.Fatal("expected error for a 31-byte hash")
	}
}
