// Command tnbench drives the teranode-bridge tx lane at saturation.
//
//	tnbench mock  -listen 127.0.0.1:20833          — propagation stand-in
//	tnbench feed  -addr 127.0.0.1:28833 -conns 8 -dur 20s
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	switch os.Args[1] {
	case "mock":
		fs := flag.NewFlagSet("mock", flag.ExitOnError)
		listen := fs.String("listen", "127.0.0.1:20833", "")
		_ = fs.Parse(os.Args[2:])
		var batches, bytes atomic.Uint64
		mux := http.NewServeMux()
		h := func(w http.ResponseWriter, r *http.Request) {
			n, _ := io.Copy(io.Discard, r.Body)
			batches.Add(1)
			bytes.Add(uint64(n))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		}
		mux.HandleFunc("/txs", h)
		mux.HandleFunc("/tx", h)
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
		go func() {
			for range time.Tick(5 * time.Second) {
				fmt.Printf("mock: batches=%d bytes=%d\n", batches.Load(), bytes.Load())
			}
		}()
		panic(http.ListenAndServe(*listen, mux))

	case "feed":
		fs := flag.NewFlagSet("feed", flag.ExitOnError)
		addr := fs.String("addr", "127.0.0.1:28833", "")
		conns := fs.Int("conns", 8, "")
		dur := fs.Duration("dur", 20*time.Second, "")
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
				for time.Now().Before(stop) {
					for i := 0; i < 4096; i++ {
						ctr++
						binary.LittleEndian.PutUint64(tx[5:], ctr) // prev txid varies
						tx[13] = cid
						binary.LittleEndian.PutUint64(tx[42:], ctr) // script varies -> unique txid
						tx[50] = cid
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
