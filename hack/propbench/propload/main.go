package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:18833/txs", "")
	conns := flag.Int("conns", 16, "")
	per := flag.Int("batch", 1024, "")
	dur := flag.Duration("dur", 10*time.Second, "")
	flag.Parse()

	var ok, bad, txs atomic.Uint64
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
	fmt.Printf("batches ok=%d bad=%d txs=%d rate=%.0f tx/s\n", ok.Load(), bad.Load(), txs.Load(), float64(txs.Load())/dur.Seconds())
}
