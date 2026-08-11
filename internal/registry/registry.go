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
package registry

import (
	"sync"
	"time"
)

// Key identifies an object by its hash.
type Key [32]byte

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

type record struct {
	dir  Direction
	when time.Time
}

// Registry is a TTL'd set of hashes with the direction they were seen in.
type Registry struct {
	mu         sync.Mutex
	seen       map[Key]record
	ttl        time.Duration
	maxEntries int
	now        func() time.Time

	hits, adds, pruned uint64
}

// New returns a registry holding at most maxEntries entries for ttl each.
func New(ttl time.Duration, maxEntries int) *Registry {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	if maxEntries <= 0 {
		maxEntries = 1 << 20
	}
	return &Registry{seen: make(map[Key]record), ttl: ttl, maxEntries: maxEntries, now: time.Now}
}

// Mark records key in direction dir and reports whether it was already known
// (with the direction it was known in). Callers use the bool to decide whether
// to act or to drop as a duplicate.
func (r *Registry) Mark(key Key, dir Direction) (prev Direction, known bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if rec, ok := r.seen[key]; ok && r.now().Sub(rec.when) <= r.ttl {
		r.hits++
		rec.when = r.now() // keep hot entries alive while they keep recurring
		r.seen[key] = rec
		return rec.dir, true
	}
	r.pruneLocked()
	r.seen[key] = record{dir: dir, when: r.now()}
	r.adds++
	return 0, false
}

// Lookup reports the direction key was seen in, without recording anything.
func (r *Registry) Lookup(key Key) (Direction, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.seen[key]
	if !ok || r.now().Sub(rec.when) > r.ttl {
		return 0, false
	}
	return rec.dir, true
}

// pruneLocked drops expired entries, and — if still at the ceiling — the oldest
// ones. Bounded work per call: a full sweep only happens when at capacity.
func (r *Registry) pruneLocked() {
	if len(r.seen) < r.maxEntries {
		return
	}
	cutoff := r.now().Add(-r.ttl)
	oldest := Key{}
	var oldestAt time.Time
	first := true
	for k, rec := range r.seen {
		if rec.when.Before(cutoff) {
			delete(r.seen, k)
			r.pruned++
			continue
		}
		if first || rec.when.Before(oldestAt) {
			oldest, oldestAt, first = k, rec.when, false
		}
	}
	if len(r.seen) >= r.maxEntries && !first {
		delete(r.seen, oldest)
		r.pruned++
	}
}

// Stats is a snapshot for logging and metrics.
type Stats struct {
	Entries            int
	Hits, Adds, Pruned uint64
}

// Stats returns a snapshot of the registry's size and counters.
func (r *Registry) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{Entries: len(r.seen), Hits: r.hits, Adds: r.adds, Pruned: r.pruned}
}
