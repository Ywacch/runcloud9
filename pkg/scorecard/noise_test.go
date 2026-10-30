package scorecard

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestSteadyStateWindows(t *testing.T) {
	t.Parallel()

	warmup := 10 * time.Second
	k := 3

	tests := []struct {
		name    string
		samples []Sample
		wantErr error
		check   func(t *testing.T, wins []Window)
	}{
		{
			name: "drops warm-up",
			samples: []Sample{
				{At: 5 * time.Second, Value: 1},
				{At: 10 * time.Second, Value: 10},
				{At: 20 * time.Second, Value: 20},
				{At: 30 * time.Second, Value: 30},
			},
			check: func(t *testing.T, wins []Window) {
				t.Helper()
				if len(wins) != k {
					t.Fatalf("windows: %d", len(wins))
				}
				if wins[0].Start != warmup {
					t.Fatalf("first start: %v", wins[0].Start)
				}
			},
		},
		{
			name: "unsorted input",
			samples: []Sample{
				{At: 30 * time.Second, Value: 30},
				{At: 10 * time.Second, Value: 10},
				{At: 20 * time.Second, Value: 20},
			},
			check: func(t *testing.T, wins []Window) {
				t.Helper()
				if wins[0].Mean != 10 || wins[2].Mean != 30 {
					t.Fatalf("means: %v %v %v", wins[0].Mean, wins[1].Mean, wins[2].Mean)
				}
			},
		},
		{
			name: "empty window errors",
			samples: []Sample{
				{At: 0, Value: 1},
				{At: 1 * time.Second, Value: 2},
				{At: 9 * time.Second, Value: 9},
			},
			wantErr: ErrInsufficientData,
		},
		{
			name: "too few after warm-up",
			samples: []Sample{
				{At: 0, Value: 1},
				{At: 10 * time.Second, Value: 2},
			},
			wantErr: ErrInsufficientData,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wins, err := SteadyStateWindows(tc.samples, warmup, k)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.check != nil {
				tc.check(t, wins)
			}
		})
	}
}

func TestCompareWindows(t *testing.T) {
	t.Parallel()

	constA := func(v float64, n int) []Window {
		out := make([]Window, n)
		for i := range out {
			out[i] = Window{Mean: v, N: 1}
		}
		return out
	}

	a := constA(10, 3)
	b := constA(10, 3)
	d, err := CompareWindows(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if !d.WithinNoise || d.Diff != 0 {
		t.Fatalf("identical constants: %+v", d)
	}

	shifted := constA(20, 3)
	d2, err := CompareWindows(a, shifted)
	if err != nil {
		t.Fatal(err)
	}
	if d2.WithinNoise {
		t.Fatalf("shifted constants should exceed noise: %+v", d2)
	}

	// Hand-built case: threshold strictly less than |diff| at boundary.
	wA := []Window{{Mean: 0}, {Mean: 0}, {Mean: 0}}
	wB := []Window{{Mean: 4}, {Mean: 4}, {Mean: 4}}
	d3, err := CompareWindows(wA, wB)
	if err != nil {
		t.Fatal(err)
	}
	if d3.Threshold != 0 {
		t.Fatalf("threshold = %v want 0", d3.Threshold)
	}
	if d3.WithinNoise {
		t.Fatalf("zero threshold requires diff==0: %+v", d3)
	}

	// Two-window sides with computed threshold; |diff| equal to threshold is NOT within noise.
	wA2 := []Window{{Mean: 1}, {Mean: 3}}
	wB2 := []Window{{Mean: 2}, {Mean: 4}}
	d4, err := CompareWindows(wA2, wB2)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(d4.Diff) >= d4.Threshold && d4.WithinNoise {
		t.Fatalf("boundary must be strict <: diff=%v threshold=%v within=%v", d4.Diff, d4.Threshold, d4.WithinNoise)
	}
}

func TestCompareWindows_TooFewWindows(t *testing.T) {
	t.Parallel()

	_, err := CompareWindows([]Window{{Mean: 1}}, []Window{{Mean: 1}, {Mean: 2}})
	if !errors.Is(err, ErrBadWindowCount) {
		t.Fatalf("err = %v", err)
	}
}
