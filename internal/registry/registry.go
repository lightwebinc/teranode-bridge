// Package registry remembers which object hashes crossed the bridge, and in
// which direction.
//
// It does two jobs that look similar but are not:
//
//   - Down (delivery): suppress re-injection of an object we already handed to
//     the cluster. A/B tunnel failover and reconnects legitimately re-deliver.
//
//   - Up (reverse path): decide whether a subtree/block the cluster just
//     accepted actually originated here. Teranode's blockchain notifications
//     carry no local-vs-remote marker — its own p2p announces every one of them
//     — so "did we deliver this to the cluster?" is the origin filter. Anything
//     registered as delivered is remote in origin and must not be pushed back
//     up; anything unseen is ours to publish.
//
// Entries expire: the question is only interesting while an object is in flight.
//
// # Structure
//
// Sharded 64 ways by the first key byte (keys are content hashes, so the
// spread is uniform), and aged GENERATIONALLY: each shard keeps a current and
// a previous map, and rotation — every ttl/2, or when the current map reaches
// half the shard's capacity — drops the previous map wholesale and demotes the
// current one. Expiry is therefore O(1) amortized per operation. The earlier
// design swept the whole map on every insert once at capacity; profiled under
// a saturated tx lane that sweep was 93% of the process's CPU. The trade is
// TTL precision: an entry now lives between ttl/2 and ttl (or less under
// capacity pressure), which is exactly as good for duplicate suppression.
package registry

import (
	"sync"
	"time"
)

// Key identifies an object by its hash.
type Key [32]byte

const shards = 64

// Direction records how a hash became known.
type Direction uint8

const (
	// Delivered means the object arrived from the fabric and was handed to the
	// cluster. Seeing it again from either side is a duplicate.
	Delivered Direction = iota + 1
	// Submitted means the object originated in the cluster and was sent up the
	// tunnel. It will come back down the delivery lanes (own-traffic exclusion
	// does not cover subtree/block classes) and must be dropped on return.
	Submitted
)

func (d Direction) String() string {
	switch d {
	case Delivered:
		return "delivered"
	case Submitted:
		return "submitted"
	default:
		return "unknown"
	}
}

type rshard struct {
	mu      sync.Mutex
	cur     map[Key]Direction
	prev    map[Key]Direction
	rotated time.Time
	cap     int // rotation threshold for cur; live set ≤ 2×cap

	hits, adds, pruned uint64
}

// Registry is a TTL'd set of hashes with the direction they were seen in.
type Registry struct {
	s   [shards]*rshard
	ttl time.Duration
	now func() time.Time
}

// New returns a registry holding at most maxEntries entries for roughly ttl
// each (an entry survives between ttl/2 and ttl).
func New(ttl time.Duration, maxEntries int) *Registry {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	if maxEntries <= 0 {
		maxEntries = 1 << 20
	}
	per := maxEntries / shards / 2 // cur+prev together stay under the share
	if per <= 0 {
		per = 1
	}
	r := &Registry{ttl: ttl, now: time.Now}
	start := r.now()
	for i := range r.s {
		r.s[i] = &rshard{
			cur:     make(map[Key]Direction),
			prev:    map[Key]Direction{},
			rotated: start,
			cap:     per,
		}
	}
	return r
}

func (r *Registry) shardOf(key Key) *rshard { return r.s[key[0]&(shards-1)] }

// rotateLocked ages the shard: prev is dropped wholesale, cur becomes prev.
func (s *rshard) rotateLocked(now time.Time) {
	s.pruned += uint64(len(s.prev))
	s.prev = s.cur
	s.cur = make(map[Key]Direction, len(s.prev))
	s.rotated = now
}

func (s *rshard) maybeRotateLocked(now time.Time, halfTTL time.Duration) {
	if now.Sub(s.rotated) >= halfTTL || len(s.cur) >= s.cap {
		s.rotateLocked(now)
	}
}

// Mark records key in direction dir and reports whether it was already known
// (with the direction it was known in). A hit refreshes the entry's age.
func (r *Registry) Mark(key Key, dir Direction) (prev Direction, known bool) {
	s := r.shardOf(key)
	now := r.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeRotateLocked(now, r.ttl/2)

	if d, ok := s.cur[key]; ok {
		s.hits++
		return d, true
	}
	if d, ok := s.prev[key]; ok {
		s.hits++
		s.cur[key] = d // refresh: promote so it survives the next rotation
		return d, true
	}
	s.cur[key] = dir
	s.adds++
	return 0, false
}

// Lookup reports the direction key was seen in, without recording anything.
func (r *Registry) Lookup(key Key) (Direction, bool) {
	s := r.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.cur[key]; ok {
		return d, true
	}
	if d, ok := s.prev[key]; ok {
		return d, true
	}
	return 0, false
}

// Stats is a snapshot for logging and metrics.
type Stats struct {
	Entries            int
	Hits, Adds, Pruned uint64
}

// Stats returns a snapshot of the registry's size and counters, aggregated
// over all shards.
func (r *Registry) Stats() Stats {
	var st Stats
	for _, s := range r.s {
		s.mu.Lock()
		st.Entries += len(s.cur) + len(s.prev)
		st.Hits += s.hits
		st.Adds += s.adds
		st.Pruned += s.pruned
		s.mu.Unlock()
	}
	return st
}
