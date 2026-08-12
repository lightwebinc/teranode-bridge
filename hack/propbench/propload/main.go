package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:18833/txs", "propagation batch endpoint")
	conns := flag.Int("conns", 16, "concurrent posting goroutines")
	// 1023, not 1024: propagation checks `totalNrTransactions >=
	// maxTransactionsPerRequest` at the top of its read loop, so a batch of
	// exactly 1024 is fully processed and THEN answered 400 — a default run
	// would report rate=0 while the server did all the work.
	per := flag.Int("batch", 1023, "transactions per request (server accepts at most 1023)")
	dur := flag.Duration("dur", 10*time.Second, "run length")
	flag.Parse()

	if *per > 1023 {
		fmt.Printf("warning: -batch %d exceeds the 1023 the server accepts; every request will be answered 400 AFTER being processed\n", *per)
	}

	var ok, bad, txs atomic.Uint64
	byStatus := &statusCounts{}
	var wg sync.WaitGroup
	stop := time.Now().Add(*dur)
	for c := 0; c < *conns; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			cl := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}}
			ctr := uint64(cid) << 40
			buf := make([]byte, 0, *per*70)
			for time.Now().Before(stop) {
				buf = buf[:0]
				for i := 0; i < *per; i++ {
					ctr++
					tx := make([]byte, 70)
					tx[0], tx[4], tx[41] = 1, 1, 9
					tx[51], tx[52], tx[53], tx[54] = 0xFF, 0xFF, 0xFF, 0xFF
					tx[55], tx[56], tx[64], tx[65] = 1, 1, 1, 0x51
					binary.LittleEndian.PutUint64(tx[5:], ctr)
					binary.LittleEndian.PutUint64(tx[42:], ctr)
					buf = append(buf, tx...)
				}
				resp, err := cl.Post(*url, "application/octet-stream", bytes.NewReader(buf))
				if err != nil {
					bad.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				byStatus.add(resp.StatusCode)
				if resp.StatusCode == 200 {
					ok.Add(1)
					txs.Add(uint64(*per))
				} else {
					bad.Add(1)
				}
			}
		}(c)
	}
	wg.Wait()
	fmt.Printf("batches ok=%d bad=%d txs=%d rate=%.0f tx/s (client-side, 200-only)\n",
		ok.Load(), bad.Load(), txs.Load(), float64(txs.Load())/dur.Seconds())
	fmt.Printf("status codes: %s\n", byStatus.String())
	fmt.Println("NOTE: this counts only what the SERVER confirmed with 200. Compare it against the")
	fmt.Println("validatortxs high-watermark delta (propbench.sh reports it) — a gap between the two")
	fmt.Println("means transactions were processed and then reported as failed.")
}

// statusCounts records the response-code mix so a run that reports zero
// throughput says WHY rather than looking like a dead server.
type statusCounts struct {
	mu sync.Mutex
	m  map[int]uint64
}

func (s *statusCounts) add(code int) {
	s.mu.Lock()
	if s.m == nil {
		s.m = make(map[int]uint64, 4)
	}
	s.m[code]++
	s.mu.Unlock()
}

func (s *statusCounts) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	codes := make([]int, 0, len(s.m))
	for c := range s.m {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	parts := make([]string, 0, len(codes))
	for _, c := range codes {
		parts = append(parts, fmt.Sprintf("%d=%d", c, s.m[c]))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}
