package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lightwebinc/teranode-bridge/internal/cache"
	"github.com/lightwebinc/teranode-bridge/internal/lanes"
	"github.com/lightwebinc/teranode-bridge/internal/registry"
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
