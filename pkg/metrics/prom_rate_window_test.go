package metrics

import "testing"

func TestPromRateWindow(t *testing.T) {
	t.Parallel()
	if PromRateWindow != "10s" {
		t.Fatalf("PromRateWindow: want 10s, got %q", PromRateWindow)
	}
	if promRateRange() != "[10s]" {
		t.Fatalf("promRateRange: want [10s], got %q", promRateRange())
	}
}
