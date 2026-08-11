// Package cache holds pushed object bytes between delivery and the moment the
// Teranode cluster pulls and validates them.
//
// This is a cache, not a store of record: the store of record is the cluster's
// own, after validation. The working set is therefore delivery-rate × validation
// lag — seconds of traffic — so entries are held by hash with a TTL and a byte
// ceiling, and eviction is a normal event rather than data loss (a missed pull
// falls back to the cluster's ordinary peer-pull path).
//
// The cache is sharded 64 ways by the first key byte. Keys are content hashes,
// so the spread is uniform and no shard's mutex sees more than ~1/64th of the
// put/get rate — a single global lock convulses long before the megaobject-per-
// second rates the tx lane is sized for.
package cache

import (
	"container/list"
	"sync"
	"time"
)

// Key identifies an object by its hash.
type Key [32]byte

const shards = 64

type entry struct {
	key    Key
	body   []byte
	class  string
	stored time.Time
	elem   *list.Element
}

// shard is one lock's worth of cache: a hash-keyed LRU with a TTL and a
// per-shard byte ceiling.
type shard struct {
	mu       sync.Mutex
	items    map[Key]*entry
	lru      *list.List // front = most recently used
	bytes    int64
	maxBytes int64
	ttl      time.Duration
	now      func() time.Time

	// counters, read via Stats
	stored, hits, misses, evicted, expired uint64
}

// Cache is safe for concurrent use: ingest writes while retrieval reads.
type Cache struct {
	s [shards]*shard
}

// Options configure a Cache. Zero values pick sane defaults.
type Options struct {
	MaxBytes int64
	TTL      time.Duration
}

// New returns a ready cache.
func New(o Options) *Cache {
	if o.MaxBytes <= 0 {
		o.MaxBytes = 1 << 30 // 1 GiB
	}
	if o.TTL <= 0 {
		o.TTL = 10 * time.Minute
	}
	per := o.MaxBytes / shards
	if per <= 0 {
		per = 1
	}
	c := &Cache{}
	for i := range c.s {
		c.s[i] = &shard{
			items:    make(map[Key]*entry),
			lru:      list.New(),
			maxBytes: per,
			ttl:      o.TTL,
			now:      time.Now,
		}
	}
	return c
}

func (c *Cache) shardOf(key Key) *shard { return c.s[key[0]&(shards-1)] }

// Put stores body under key. Storing an existing key refreshes it and keeps the
// original bytes — re-delivery of an object we already hold is a no-op, which is
// what makes A/B failover and reconnects harmless.
//
// The bytes are COPIED. Callers hand in the slice objfmt.Reader.Next returned,
// which aliases the reader's buffer and is overwritten by later objects on the
// same connection — retaining it uncopied corrupts cached entries in place,
// and the retrieval plane would serve those bytes to the cluster.
func (c *Cache) Put(key Key, class string, body []byte) {
	s := c.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		e.stored = s.now()
		s.lru.MoveToFront(e.elem)
		return
	}
	body = append([]byte(nil), body...)
	s.putOwnedLocked(key, class, body)
}

// PutOwned stores body WITHOUT copying: ownership transfers to the cache and
// the caller must never mutate the slice again. It exists for the tx hot path,
// where the lane already makes one immutable copy per object and a second
// copy per million transactions a second is pure GC pressure. Every other
// caller wants Put.
func (c *Cache) PutOwned(key Key, class string, body []byte) {
	s := c.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[key]; ok {
		e.stored = s.now()
		s.lru.MoveToFront(e.elem)
		return
	}
	s.putOwnedLocked(key, class, body)
}

func (s *shard) putOwnedLocked(key Key, class string, body []byte) {
	e := &entry{key: key, body: body, class: class, stored: s.now()}
	e.elem = s.lru.PushFront(e)
	s.items[key] = e
	s.bytes += int64(len(body))
	s.stored++
	s.evictLocked()
}

// Get returns the stored body, or ok=false if absent or expired.
func (c *Cache) Get(key Key) (body []byte, class string, ok bool) {
	s := c.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		s.misses++
		return nil, "", false
	}
	if s.now().Sub(e.stored) > s.ttl {
		s.removeLocked(e)
		s.expired++
		s.misses++
		return nil, "", false
	}
	s.lru.MoveToFront(e.elem)
	s.hits++
	return e.body, e.class, true
}

// Has reports presence without counting a hit or miss.
func (c *Cache) Has(key Key) bool {
	s := c.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.items[key]
	return ok && s.now().Sub(e.stored) <= s.ttl
}

func (s *shard) evictLocked() {
	for s.bytes > s.maxBytes {
		back := s.lru.Back()
		if back == nil {
			return
		}
		s.removeLocked(back.Value.(*entry))
		s.evicted++
	}
}

func (s *shard) removeLocked(e *entry) {
	s.lru.Remove(e.elem)
	delete(s.items, e.key)
	s.bytes -= int64(len(e.body))
}

// Stats is a snapshot for logging and metrics.
type Stats struct {
	Entries                                int
	Bytes, MaxBytes                        int64
	Stored, Hits, Misses, Evicted, Expired uint64
}

// Stats returns a snapshot of the cache's size and counters, aggregated over
// all shards.
func (c *Cache) Stats() Stats {
	var st Stats
	for _, s := range c.s {
		s.mu.Lock()
		st.Entries += len(s.items)
		st.Bytes += s.bytes
		st.MaxBytes += s.maxBytes
		st.Stored += s.stored
		st.Hits += s.hits
		st.Misses += s.misses
		st.Evicted += s.evicted
		st.Expired += s.expired
		s.mu.Unlock()
	}
	return st
}
