package metrics

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRingBytes_SumsMarshaledSnapshots(t *testing.T) {
	t.Parallel()

	c := &MetricsCollector{}
	nsA := "ns-a"
	nsB := "ns-b"
	snapA := DeploymentMetrics{
		Timestamp:  time.Now(),
		Namespace:  nsA,
		Components: map[string]ComponentMetrics{},
	}
	snapB := DeploymentMetrics{
		Timestamp:  time.Now(),
		Namespace:  nsB,
		Components: map[string]ComponentMetrics{"app-server": {Component: "app-server"}},
	}
	c.AppendSessionSnapshots(nsA, snapA)
	c.AppendSessionSnapshots(nsB, snapB)

	bA, err := json.Marshal(snapA)
	if err != nil {
		t.Fatal(err)
	}
	bB, err := json.Marshal(snapB)
	if err != nil {
		t.Fatal(err)
	}
	want := len(bA) + len(bB)
	if got := c.RingBytes(); got != want {
		t.Fatalf("RingBytes: want %d, got %d", want, got)
	}

	c.Cleanup(nsA)
	if got := c.RingBytes(); got != len(bB) {
		t.Fatalf("after cleanup ns-a: want %d, got %d", len(bB), got)
	}
}
