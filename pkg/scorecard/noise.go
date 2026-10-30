package scorecard

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

// Sample is one metric reading at a session-relative time.
type Sample struct {
	At    time.Duration // since run t=0
	Value float64
}

// Window is one steady-state slice of a run.
type Window struct {
	Start, End time.Duration
	Mean       float64
	N          int // samples in the window
}

const DefaultWindows = 10

var (
	ErrInsufficientData = errors.New("scorecard: insufficient data")
	ErrBadWindowCount   = errors.New("scorecard: window count must be >= 2")
)

// Delta compares two runs' window means under the S-5 noise rule.
type Delta struct {
	MeanA, MeanB float64
	Diff         float64 // MeanB - MeanA
	Threshold    float64 // 2 * sqrt(varA/kA + varB/kB)
	WithinNoise  bool
}

// SteadyStateWindows drops samples before warmup and splits the rest into k equal-time windows.
// A window with no samples is an error, not a zero.
func SteadyStateWindows(samples []Sample, warmup time.Duration, k int) ([]Window, error) {
	if k < 1 {
		return nil, fmt.Errorf("scorecard: window count must be >= 1")
	}
	if len(samples) == 0 {
		return nil, ErrInsufficientData
	}

	copied := append([]Sample(nil), samples...)
	sort.Slice(copied, func(i, j int) bool {
		return copied[i].At < copied[j].At
	})

	steady := make([]Sample, 0, len(copied))
	for _, s := range copied {
		if s.At >= warmup {
			steady = append(steady, s)
		}
	}
	if len(steady) < k {
		return nil, ErrInsufficientData
	}

	lastAt := steady[len(steady)-1].At
	span := lastAt - warmup
	if span < 0 {
		return nil, ErrInsufficientData
	}

	width := time.Duration(int64(span) / int64(k))
	if width == 0 {
		width = 1
	}

	acc := make([]struct {
		sum   float64
		count int
	}, k)
	for _, s := range steady {
		idx := int((s.At - warmup) / width)
		if idx >= k {
			idx = k - 1
		}
		if idx < 0 {
			idx = 0
		}
		acc[idx].sum += s.Value
		acc[idx].count++
	}

	out := make([]Window, k)
	for i := 0; i < k; i++ {
		if acc[i].count == 0 {
			return nil, ErrInsufficientData
		}
		start := warmup + time.Duration(i)*width
		end := warmup + time.Duration(i+1)*width
		if i == k-1 {
			end = lastAt
		}
		out[i] = Window{
			Start: start,
			End:   end,
			Mean:  acc[i].sum / float64(acc[i].count),
			N:     acc[i].count,
		}
	}
	return out, nil
}

// CompareWindows applies the S-5 rule: within noise when |meanA-meanB| < 2*sqrt(varA/kA + varB/kB).
// Variances are sample variances (n-1) of the window means.
func CompareWindows(a, b []Window) (Delta, error) {
	if len(a) < 2 || len(b) < 2 {
		return Delta{}, ErrBadWindowCount
	}

	meanA := meanOfMeans(a)
	meanB := meanOfMeans(b)
	diff := meanB - meanA

	varA := sampleVariance(windowMeans(a))
	varB := sampleVariance(windowMeans(b))
	kA := float64(len(a))
	kB := float64(len(b))

	threshold := 2 * math.Sqrt(varA/kA+varB/kB)
	within := false
	if threshold == 0 {
		within = diff == 0
	} else {
		within = math.Abs(diff) < threshold
	}

	return Delta{
		MeanA:       meanA,
		MeanB:       meanB,
		Diff:        diff,
		Threshold:   threshold,
		WithinNoise: within,
	}, nil
}

func meanOfMeans(w []Window) float64 {
	vals := windowMeans(w)
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func windowMeans(w []Window) []float64 {
	out := make([]float64, len(w))
	for i := range w {
		out[i] = w[i].Mean
	}
	return out
}

func sampleVariance(vals []float64) float64 {
	n := len(vals)
	if n < 2 {
		return 0
	}
	mean := 0.0
	for _, v := range vals {
		mean += v
	}
	mean /= float64(n)
	var sumSq float64
	for _, v := range vals {
		d := v - mean
		sumSq += d * d
	}
	return sumSq / float64(n-1)
}
