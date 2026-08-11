package cache

import (
	"sync"
	"time"
)

// Generational is the tx-lane variant of the cache: a sharded two-generation
// map with wholesale rotation instead of an LRU.
//
// The LRU cache costs ~340 bytes of heap per 70-byte transaction (entry
// struct, list element, map bucket) and its pointer graph is what the garbage
// collector scans; at a million puts per second the process pins its memory
// limit and collapses into continuous GC. Here an entry is map[Key][]byte and
// nothing else: no per-entry timestamps, no list, no struct. Aging is
// generational — every ttl/2, or when the current generation reaches half the
// byte budget, the previous generation is dropped wholesale and the current
// one demoted — so eviction is O(1) amortized and precision degrades only in
// the direction that does not matter: an entry lives between ttl/2 and ttl.
//
// The semantics fit exactly what the tx cache is for: serving subtree_data
// member lookups for subtrees completed in the last few seconds. It is a
// recency window, not a store.
type Generational struct {
	s [shards]*gshard
}

type gshard struct {
	mu        sync.Mutex
	cur, prev map[Key][]byte
	curBytes  int64
	prevBytes int64
	maxBytes  int64 // rotation threshold for cur; live total ≤ 2× this
	ttl       time.Duration
	rotated   time.Time
	now       func() time.Time

	stored, hits, misses, evicted uint64
}

// NewGenerational returns a generational cache. MaxBytes bounds the LIVE total
// (both generations); TTL bounds entry age at rotation cadence ttl/2.
func NewGenerational(o Options) *Generational {
	if o.MaxBytes <= 0 {
		o.MaxBytes = 1 << 30
	}
	if o.TTL <= 0 {
		o.TTL = 10 * time.Minute
	}
	per := o.MaxBytes / shards / 2
	if per <= 0 {
		per = 1
	}
	g := &Generational{}
	start := time.Now()
	for i := range g.s {
		g.s[i] = &gshard{
			cur:      make(map[Key][]byte),
			prev:     map[Key][]byte{},
			maxBytes: per,
			ttl:      o.TTL,
			rotated:  start,
			now:      time.Now,
		}
	}
	return g
}

func (g *Generational) shardOf(key Key) *gshard { return g.s[key[0]&(shards-1)] }

func (s *gshard) rotateLocked(now time.Time) {
	s.evicted += uint64(len(s.prev))
	s.prev = s.cur
	s.prevBytes = s.curBytes
	s.cur = make(map[Key][]byte, len(s.prev))
	s.curBytes = 0
	s.rotated = now
}

// PutOwned stores body without copying; ownership transfers to the cache.
func (g *Generational) PutOwned(key Key, _ string, body []byte) {
	s := g.shardOf(key)
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Sub(s.rotated) >= s.ttl/2 || s.curBytes >= s.maxBytes {
		s.rotateLocked(now)
	}
	if _, ok := s.cur[key]; ok {
		return
	}
	s.cur[key] = body
	s.curBytes += int64(len(body))
	s.stored++
}

// Put stores a copy of body (for callers whose slice aliases a reader buffer).
func (g *Generational) Put(key Key, class string, body []byte) {
	g.PutOwned(key, class, append([]byte(nil), body...))
}

// Get returns the stored body. The class of every entry is "tx".
func (g *Generational) Get(key Key) (body []byte, class string, ok bool) {
	s := g.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.cur[key]; ok {
		s.hits++
		return b, "tx", true
	}
	if b, ok := s.prev[key]; ok {
		s.hits++
		return b, "tx", true
	}
	s.misses++
	return nil, "", false
}

// Has reports presence without counting a hit or miss.
func (g *Generational) Has(key Key) bool {
	s := g.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cur[key]; ok {
		return true
	}
	_, ok := s.prev[key]
	return ok
}

// Stats returns a snapshot aggregated over all shards. Evicted counts entries
// dropped by generation rotation.
func (g *Generational) Stats() Stats {
	var st Stats
	for _, s := range g.s {
		s.mu.Lock()
		st.Entries += len(s.cur) + len(s.prev)
		st.Bytes += s.curBytes + s.prevBytes
		st.MaxBytes += s.maxBytes * 2
		st.Stored += s.stored
		st.Hits += s.hits
		st.Misses += s.misses
		st.Evicted += s.evicted
		s.mu.Unlock()
	}
	return st
}
