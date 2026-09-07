package registry

import "testing"

func key(b byte, n int) Key {
	var k Key
	k[0] = b // pins the shard
	k[1] = byte(n)
	k[2] = byte(n >> 8)
	return k
}

// TestMarkDedupes pins the basic contract: first Mark is unknown, the second
// reports the original direction.
func TestMarkDedupes(t *testing.T) {
	r := New(0, 0)
	k := key(1, 1)
	if _, known := r.Mark(k, Delivered); known {
		t.Fatal("fresh key reported known")
	}
	if d, known := r.Mark(k, Submitted); !known || d != Delivered {
		t.Fatalf("second Mark = (%v, %v), want (Delivered, true)", d, known)
	}
	if d, ok := r.Lookup(k); !ok || d != Delivered {
		t.Fatalf("Lookup = (%v, %v)", d, ok)
	}
}

// TestGenerationalEviction pins the O(1) aging design: filling a shard past
// two rotations evicts the oldest generation without any per-insert sweep,
// and a refreshed (re-Marked) entry survives one rotation.
func TestGenerationalEviction(t *testing.T) {
	r := New(0, 64*4) // per-shard cap = 4*... /2 => tiny: rotation every 2 inserts
	shardCap := r.s[0].cap

	old := key(0, 0)
	r.Mark(old, Delivered)
	refreshed := key(0, 1)
	r.Mark(refreshed, Submitted)

	// Push enough inserts through the SAME shard to rotate twice, refreshing
	// `refreshed` between rotations so promotion keeps it alive.
	for i := 2; i < 2+2*shardCap; i++ {
		r.Mark(key(0, i), Delivered)
		r.Mark(refreshed, Submitted) // hit → promoted into cur each time
	}

	if _, ok := r.Lookup(old); ok {
		t.Fatal("unrefreshed entry survived two rotations")
	}
	if d, ok := r.Lookup(refreshed); !ok || d != Submitted {
		t.Fatalf("refreshed entry lost: (%v, %v)", d, ok)
	}
	if s := r.Stats(); s.Pruned == 0 {
		t.Fatal("rotation never pruned anything")
	}
}
