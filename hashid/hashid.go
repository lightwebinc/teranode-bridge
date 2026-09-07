// Package hashid handles the one byte-order trap that runs through this whole
// bridge.
//
// Bitcoin hashes exist in two orders. On the wire — inside frames, subtree node
// lists, block headers — they are "internal" order. Everywhere a human or an API
// sees them (RPC output, URL path segments, the hash field of a Kafka
// announcement) they are the byte-REVERSED "display" order. Teranode parses
// announced and requested hashes with chainhash.NewHashFromStr, which reverses;
// so a hash taken straight off the wire and hex-encoded would be silently wrong
// — the announcement would name an object nobody can find.
//
// Every conversion between the two orders goes through this package.
package hashid

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Hash is a hash in internal (wire) byte order.
type Hash [32]byte

// FromWire copies 32 bytes of internal-order hash.
func FromWire(b []byte) (Hash, error) {
	var h Hash
	if len(b) < 32 {
		return h, fmt.Errorf("hashid: need 32 bytes, got %d", len(b))
	}
	copy(h[:], b[:32])
	return h, nil
}

// DoubleSHA256 returns SHA256d(b) in internal order — the block hash of an
// 80-byte header, for example.
func DoubleSHA256(b []byte) Hash {
	first := sha256.Sum256(b)
	return Hash(sha256.Sum256(first[:]))
}

// Display returns the reversed hex form: what Teranode announces, logs, and puts
// in a URL path.
func (h Hash) Display() string {
	var rev [32]byte
	for i := 0; i < 32; i++ {
		rev[i] = h[31-i]
	}
	return hex.EncodeToString(rev[:])
}

// Wire returns the internal-order bytes.
func (h Hash) Wire() []byte { return h[:] }

// ParseDisplay converts a display-order hex string (64 chars, as it appears in a
// URL path or an announcement) back to internal order.
func ParseDisplay(s string) (Hash, error) {
	var h Hash
	if len(s) != 64 {
		return h, fmt.Errorf("hashid: want 64 hex chars, got %d", len(s))
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return h, fmt.Errorf("hashid: %w", err)
	}
	for i := 0; i < 32; i++ {
		h[i] = raw[31-i]
	}
	return h, nil
}
