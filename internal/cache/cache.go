// Package cache holds pushed object bytes between delivery and the moment the
// Teranode cluster pulls and validates them.
//
// This is a cache, not a store of record: the store of record is the cluster's
// own, after validation. The working set is therefore delivery-rate × validation
// lag — seconds of traffic — so entries are held by hash with a TTL and a byte
// ceiling, and eviction is a normal event rather than data loss (a missed pull
// falls back to the cluster's ordinary peer-pull path).
package cache

import (
	"container/list"
	"sync"
	"time"
)

// Key identifies an object by its hash.
type Key [32]byte

type entry struct {
	key    Key
	body   []byte
	class  string
	stored time.Time
	elem   *list.Element
}

// Cache is a hash-keyed LRU with a TTL and a total-bytes ceiling. Safe for
// concurrent use: ingest writes while retrieval reads.
type Cache struct {
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
	return &Cache{
		items:    make(map[Key]*entry),
		lru:      list.New(),
		maxBytes: o.MaxBytes,
		ttl:      o.TTL,
		now:      time.Now,
	}
}

// Put stores body under key. Storing an existing key refreshes it and keeps the
// original bytes — re-delivery of an object we already hold is a no-op, which is
// what makes A/B failover and reconnects harmless.
func (c *Cache) Put(key Key, class string, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.items[key]; ok {
		e.stored = c.now()
		c.lru.MoveToFront(e.elem)
		return
	}
	e := &entry{key: key, body: body, class: class, stored: c.now()}
	e.elem = c.lru.PushFront(e)
	c.items[key] = e
	c.bytes += int64(len(body))
	c.stored++
	c.evictLocked()
}

// Get returns the stored body, or ok=false if absent or expired.
func (c *Cache) Get(key Key) (body []byte, class string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, "", false
	}
	if c.now().Sub(e.stored) > c.ttl {
		c.removeLocked(e)
		c.expired++
		c.misses++
		return nil, "", false
	}
	c.lru.MoveToFront(e.elem)
	c.hits++
	return e.body, e.class, true
}

// Has reports presence without counting a hit or miss.
func (c *Cache) Has(key Key) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	return ok && c.now().Sub(e.stored) <= c.ttl
}

func (c *Cache) evictLocked() {
	for c.bytes > c.maxBytes {
		back := c.lru.Back()
		if back == nil {
			return
		}
		c.removeLocked(back.Value.(*entry))
		c.evicted++
	}
}

func (c *Cache) removeLocked(e *entry) {
	c.lru.Remove(e.elem)
	delete(c.items, e.key)
	c.bytes -= int64(len(e.body))
}

// Stats is a snapshot for logging and metrics.
type Stats struct {
	Entries                                int
	Bytes, MaxBytes                        int64
	Stored, Hits, Misses, Evicted, Expired uint64
}

// Stats returns a snapshot of the cache's size and counters.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Entries: len(c.items), Bytes: c.bytes, MaxBytes: c.maxBytes,
		Stored: c.stored, Hits: c.hits, Misses: c.misses,
		Evicted: c.evicted, Expired: c.expired,
	}
}
