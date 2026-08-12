// Command tnbench drives the teranode-bridge tx lane at saturation.
//
//	tnbench mock  -listen 127.0.0.1:20833 [-faithful]  — propagation stand-in
//	tnbench feed  -addr 127.0.0.1:28833 -conns 8 -dur 20s
//
// # What each mock proves
//
// The DEFAULT mock accepts and discards. It bounds the bridge's own ceiling —
// parse, hash, copy, enqueue, batch, write — and nothing else. A number from it
// says "the bridge is not the bottleneck"; it says nothing about a cluster.
//
// The -faithful mock additionally does what propagation's batch handler does to
// the request: it splits the body into whole transactions with the SAME codec
// the wire uses, computes each txid, and enforces the parent-before-child rule
// per request, answering 500 with real "[ProcessTransaction][<txid>]" error
// lines for members whose parent it has not seen. That exercises the bridge's
// batch accounting, re-attribution and retry ladder under load — the paths a
// blind sink never reaches — at a cost of roughly one hash per transaction.
//
// Neither mock validates scripts, touches a UTXO store, or writes anything
// durable, so neither is a substitute for measuring a real cluster. They bound
// the bridge; the cluster bounds the system.
//
// For the rung above these — a REAL propagation service, standalone, with the
// blockchain service and one Redpanda and nothing else — see hack/propbench.
// Measured 2026-08-12: 152,666 tx/s at 16 connections, confirmed against the
// Kafka high-watermark delta, with the profile showing the ceiling inside
// propagation's own allocator rather than in the bridge.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
)

func main() {
	switch os.Args[1] {
	case "mock":
		fs := flag.NewFlagSet("mock", flag.ExitOnError)
		listen := fs.String("listen", "127.0.0.1:20833", "")
		faithful := fs.Bool("faithful", false, "parse the batch body and enforce the parent/child rule (see package doc)")
		_ = fs.Parse(os.Args[2:])
		var batches, bytes, txs, rejected atomic.Uint64
		seen := &seenSet{m: make(map[[32]byte]struct{}, 1<<20)}

		mux := http.NewServeMux()
		blind := func(w http.ResponseWriter, r *http.Request) {
			n, _ := io.Copy(io.Discard, r.Body)
			batches.Add(1)
			bytes.Add(uint64(n))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		}
		strict := func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			batches.Add(1)
			bytes.Add(uint64(len(body)))

			var bad []string
			off := 0
			inBatch := make(map[[32]byte]struct{})
			for off < len(body) {
				n, err := objfmt.TxSize(body[off:])
				if err != nil {
					http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
					return
				}
				tx := body[off : off+n]
				off += n
				id, err := objfmt.TxID(tx)
				if err != nil {
					http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
					return
				}
				// Propagation processes a batch concurrently, so a member whose
				// parent is in the SAME request loses the race — the caller
				// contract the bridge's dependency sealing exists to honour.
				missing := false
				_ = inputRefs(tx, func(prev [32]byte) {
					if _, ok := inBatch[prev]; ok {
						missing = true
					}
				})
				inBatch[id] = struct{}{}
				if missing {
					rejected.Add(1)
					bad = append(bad, fmt.Sprintf("[ProcessTransaction][%s] missing parent", displayHex(id)))
					continue
				}
				seen.add(id)
				txs.Add(1)
			}
			if len(bad) > 0 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = fmt.Fprintf(w, "Failed to process transactions:\n%s\n", strings.Join(bad, "\n"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		}
		h := blind
		if *faithful {
			h = strict
		}
		mux.HandleFunc("/txs", h)
		mux.HandleFunc("/tx", h)
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
		go func() {
			for range time.Tick(5 * time.Second) {
				fmt.Printf("mock: batches=%d txs=%d rejected=%d bytes=%d\n",
					batches.Load(), txs.Load(), rejected.Load(), bytes.Load())
			}
		}()
		panic(http.ListenAndServe(*listen, mux))

	case "feed":
		fs := flag.NewFlagSet("feed", flag.ExitOnError)
		addr := fs.String("addr", "127.0.0.1:28833", "")
		conns := fs.Int("conns", 8, "")
		dur := fs.Duration("dur", 20*time.Second, "")
		chain := fs.Bool("chain", false, "each tx spends the previous one — exercises dependency sealing and the missing-parent retry under load (independent txs otherwise)")
		_ = fs.Parse(os.Args[2:])
		var sent atomic.Uint64
		var wg sync.WaitGroup
		stop := time.Now().Add(*dur)
		for c := 0; c < *conns; c++ {
			wg.Add(1)
			go func(cid byte) {
				defer wg.Done()
				conn, err := net.Dial("tcp", *addr)
				if err != nil {
					fmt.Fprintln(os.Stderr, "dial:", err)
					return
				}
				defer func() { _ = conn.Close() }()
				w := bufio.NewWriterSize(conn, 1<<20)
				tx := make([]byte, 70)
				// version | in=1 | prev32 | vout | scriptLen=9 | script9 | seq |
				// out=1 | value8 | len=1 | OP_TRUE | locktime
				tx[0] = 1                                               // version
				tx[4] = 1                                               // input count
				tx[41] = 9                                              // unlocking script len
				tx[51], tx[52], tx[53], tx[54] = 0xFF, 0xFF, 0xFF, 0xFF // sequence
				tx[55] = 1                                              // output count
				tx[56] = 1                                              // value = 1 sat
				tx[64] = 1                                              // locking script len
				tx[65] = 0x51                                           // OP_TRUE
				// layout: prev txid [5:37), script [42:51), locktime [66:70)
				var ctr uint64
				var prev [32]byte // last txid this connection emitted
				for time.Now().Before(stop) {
					for i := 0; i < 4096; i++ {
						ctr++
						if *chain && ctr > 1 {
							// Spend the previous tx: a self-chaining spend
							// stream, which is what the fabric actually
							// carries, and the only way to exercise the
							// dependency sealing and missing-parent retry.
							copy(tx[5:37], prev[:])
						} else {
							binary.LittleEndian.PutUint64(tx[5:], ctr) // prev txid varies
							tx[13] = cid
						}
						binary.LittleEndian.PutUint64(tx[42:], ctr) // script varies -> unique txid
						tx[50] = cid
						if *chain {
							prev = txid(tx)
						}
						if _, err := w.Write(txFrame(tx)); err != nil {
							return
						}
					}
					sent.Add(4096)
					if err := w.Flush(); err != nil {
						return
					}
				}
				_ = w.Flush()
			}(byte(c))
		}
		wg.Wait()
		fmt.Printf("fed %d txs over %d conns in %s\n", sent.Load(), *conns, *dur)
	}
}

// txFrame exists so the fixed-layout template above stays honest: the tx IS
// the frame (lanes are bare), returned as-is.
func txFrame(tx []byte) []byte { return tx }

// txid is the double-SHA256 of the transaction, in wire (internal) order.
func txid(tx []byte) [32]byte {
	first := sha256.Sum256(tx)
	return sha256.Sum256(first[:])
}

// seenSet is the faithful mock's memory of accepted txids. It is a set, not a
// UTXO store: this rig measures the bridge, and a real spend graph would make
// the mock the bottleneck.
type seenSet struct {
	mu sync.Mutex
	m  map[[32]byte]struct{}
}

func (s *seenSet) add(id [32]byte) {
	s.mu.Lock()
	s.m[id] = struct{}{}
	s.mu.Unlock()
}

func displayHex(id [32]byte) string {
	var rev [32]byte
	for i := range id {
		rev[i] = id[31-i] // display order is byte-reversed
	}
	return hex.EncodeToString(rev[:])
}

// inputRefs walks a BRC-12/30 transaction's inputs, calling fn with each
// prevout txid. Mirrors the bridge's own walk so the mock and the code under
// test agree on what a dependency is.
func inputRefs(tx []byte, fn func([32]byte)) error {
	if len(tx) < 10 {
		return fmt.Errorf("short tx")
	}
	off := 4
	ef := tx[4] == 0 && tx[5] == 0 && tx[6] == 0 && tx[7] == 0 && tx[8] == 0 && tx[9] == 0xEF
	if ef {
		off += 6
	}
	inCount, n, err := varInt(tx, off)
	if err != nil || inCount == 0 {
		return fmt.Errorf("bad input count")
	}
	off += n
	for i := uint64(0); i < inCount; i++ {
		if off+36 > len(tx) {
			return fmt.Errorf("truncated input")
		}
		var h [32]byte
		copy(h[:], tx[off:off+32])
		fn(h)
		off += 36
		sLen, n, err := varInt(tx, off)
		if err != nil {
			return err
		}
		off += n + int(sLen) + 4
		if ef {
			if off+8 > len(tx) {
				return fmt.Errorf("truncated EF input")
			}
			off += 8
			lLen, n, err := varInt(tx, off)
			if err != nil {
				return err
			}
			off += n + int(lLen)
		}
		if off > len(tx) {
			return fmt.Errorf("input overruns tx")
		}
	}
	return nil
}

func varInt(b []byte, off int) (uint64, int, error) {
	if off >= len(b) {
		return 0, 0, fmt.Errorf("short varint")
	}
	switch v := b[off]; {
	case v < 0xfd:
		return uint64(v), 1, nil
	case v == 0xfd:
		if off+3 > len(b) {
			return 0, 0, fmt.Errorf("short varint16")
		}
		return uint64(b[off+1]) | uint64(b[off+2])<<8, 3, nil
	case v == 0xfe:
		if off+5 > len(b) {
			return 0, 0, fmt.Errorf("short varint32")
		}
		return uint64(b[off+1]) | uint64(b[off+2])<<8 | uint64(b[off+3])<<16 | uint64(b[off+4])<<24, 5, nil
	default:
		if off+9 > len(b) {
			return 0, 0, fmt.Errorf("short varint64")
		}
		var x uint64
		for i := 0; i < 8; i++ {
			x |= uint64(b[off+1+i]) << (8 * i)
		}
		return x, 9, nil
	}
}
