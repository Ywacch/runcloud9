package loadgen

import (
	"os"
	"testing"
)

func TestMaxWorkersForRPS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rps  int
		want uint64
	}{
		{name: "zero uses floor", rps: 0, want: 50},
		{name: "low rps uses floor", rps: 10, want: 50},
		{name: "50 rps", rps: 50, want: 100},
		{name: "100 rps", rps: 100, want: 200},
		{name: "500 rps", rps: 500, want: 1000},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := MaxWorkersForRPS(tc.rps); got != tc.want {
				t.Fatalf("MaxWorkersForRPS(%d) = %d, want %d", tc.rps, got, tc.want)
			}
		})
	}
}

func TestMaxWorkersForRPS_EnvOverride(t *testing.T) {
	t.Setenv("LOADGEN_MAX_WORKERS", "777")
	if got := MaxWorkersForRPS(500); got != 777 {
		t.Fatalf("override: got %d want 777", got)
	}
}

func TestInitialWorkersForRPS(t *testing.T) {
	t.Parallel()
	_ = os.Unsetenv("LOADGEN_MAX_WORKERS")

	if got := InitialWorkersForRPS(500); got != 250 {
		t.Fatalf("InitialWorkersForRPS(500) = %d, want 250", got)
	}
	if got := InitialWorkersForRPS(10); got != 12 {
		t.Fatalf("InitialWorkersForRPS(10) = %d, want 12", got)
	}
}

func TestAppendRecentInterval_Cap(t *testing.T) {
	t.Parallel()
	var buf []LoadIntervalStats
	for i := 0; i < RecentIntervalsCap+3; i++ {
		buf = appendRecentInterval(buf, LoadIntervalStats{P99Ms: float64(i)})
	}
	if len(buf) != RecentIntervalsCap {
		t.Fatalf("len = %d, want %d", len(buf), RecentIntervalsCap)
	}
	if buf[0].P99Ms != 3 {
		t.Fatalf("oldest kept = %v, want 3", buf[0].P99Ms)
	}
}
