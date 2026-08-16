package loadgen

import (
	"math"
	"testing"
)

func TestLatencyRing_EmptySnapshot(t *testing.T) {
	t.Parallel()
	var r LatencyRing
	if got := r.Snapshot(); got != nil {
		t.Fatalf("Snapshot() = %v, want nil", got)
	}
}

func TestLatencyRing_PushAndSnapshotOrder(t *testing.T) {
	t.Parallel()
	var r LatencyRing
	for i := 1; i <= 5; i++ {
		r.Push(float64(i * 10))
	}
	got := r.Snapshot()
	want := []float64{10, 20, 30, 40, 50}
	if len(got) != len(want) {
		t.Fatalf("len = %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("[%d] = %v want %v", i, got[i], want[i])
		}
	}
}

func TestLatencyRing_WrapMaintainsFIFO(t *testing.T) {
	t.Parallel()
	var r LatencyRing
	for i := 1; i <= RecentLatenciesCap+5; i++ {
		r.Push(float64(i))
	}
	got := r.Snapshot()
	if len(got) != RecentLatenciesCap {
		t.Fatalf("len = %d want %d", len(got), RecentLatenciesCap)
	}
	wantFirst := float64(6) // dropped 1..5
	wantLast := float64(RecentLatenciesCap + 5)
	if got[0] != wantFirst {
		t.Fatalf("oldest = %v want %v", got[0], wantFirst)
	}
	if got[len(got)-1] != wantLast {
		t.Fatalf("newest = %v want %v", got[len(got)-1], wantLast)
	}
}

func TestLatencyRing_IgnoresInvalid(t *testing.T) {
	t.Parallel()
	var r LatencyRing
	r.Push(-1)
	r.Push(math.NaN())
	r.Push(math.Inf(1))
	r.Push(12.5)
	got := r.Snapshot()
	if len(got) != 1 || got[0] != 12.5 {
		t.Fatalf("got %v want [12.5]", got)
	}
}
