package metrics

import (
	"testing"
	"time"
)

func TestRingBuffer_CapAt300(t *testing.T) {
	t.Parallel()
	var rb RingBuffer
	for i := 0; i < 400; i++ {
		rb.Add(DeploymentMetrics{Namespace: "ns", Timestamp: time.Unix(int64(i), 0)})
	}
	if rb.Len() != 300 {
		t.Fatalf("Len: want 300, got %d", rb.Len())
	}
	all := rb.All()
	if len(all) != 300 {
		t.Fatalf("All: want 300, got %d", len(all))
	}
	// Oldest after overflow should be snapshot 100 (first 100 dropped).
	if all[0].Timestamp.Unix() != 100 {
		t.Fatalf("oldest: want unix 100, got %v", all[0].Timestamp)
	}
	if all[299].Timestamp.Unix() != 399 {
		t.Fatalf("newest: want unix 399, got %v", all[299].Timestamp)
	}
}

func TestRingBuffer_DownsampledHistory_KeepsEvents(t *testing.T) {
	t.Parallel()
	var rb RingBuffer
	for i := 0; i < 8; i++ {
		dm := DeploymentMetrics{Timestamp: time.Unix(int64(i), 0)}
		if i == 1 {
			dm.Events = []Event{{Type: "action", Title: "x"}}
		}
		rb.Add(dm)
	}
	ds := rb.DownsampledHistory()
	want := 3
	if len(ds) != want {
		t.Fatalf("downsampled len: want %d, got %d (%v)", want, len(ds), len(ds))
	}
	if ds[1].Events[0].Title != "x" {
		t.Fatalf("event snapshot preserved")
	}
}

func TestRingBuffer_ConcurrentAddAll(t *testing.T) {
	t.Parallel()
	var rb RingBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			rb.Add(DeploymentMetrics{Timestamp: time.Unix(int64(i), 0)})
		}
	}()
	for i := 0; i < 500; i++ {
		_ = rb.All()
		_ = rb.Len()
		_ = rb.DownsampledHistory()
	}
	<-done
	if rb.Len() != 300 {
		t.Fatalf("after concurrent writes: want Len 300, got %d", rb.Len())
	}
}
