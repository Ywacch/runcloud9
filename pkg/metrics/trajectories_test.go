package metrics

import (
	"testing"
	"time"
)

func TestBuildTrajectories_SlopeDirectionInflection(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	// cache hit rate: 90 → 80 → 70 → 50 (down, inflection at index 2 when slope steepens after flat-ish)
	// Use clear V shape for inflection: 10, 20, 30, 20, 10 → inflection at peak index 2
	hist := make([]DeploymentMetrics, 5)
	hitRates := []struct{ hits, misses float64 }{
		{10, 90}, // 10%
		{20, 80}, // 20%
		{30, 70}, // 30%
		{20, 80}, // 20%
		{10, 90}, // 10%
	}
	p99s := []float64{0.01, 0.02, 0.03, 0.04, 0.05} // rising ms
	for i := range hist {
		hist[i] = DeploymentMetrics{
			Timestamp: t0.Add(time.Duration(i) * 2 * time.Second),
			Components: map[string]ComponentMetrics{
				"cache": {
					CacheHitsLast9m:   hitRates[i].hits,
					CacheMissesLast9m: hitRates[i].misses,
				},
				"app-server": {
					P99Latency: p99s[i],
					ErrorRate:  0.1,
				},
				"db": {
					CPUUsage: 0.5,
					CPULimit: 1.0,
				},
			},
		}
	}

	got := BuildTrajectories(hist)
	cache, ok := got["cache_hit_rate"].(map[string]any)
	if !ok {
		t.Fatalf("missing cache_hit_rate: %#v", got)
	}
	if cache["direction"] != "flat" && cache["direction"] != "down" && cache["direction"] != "up" {
		t.Fatalf("unexpected direction: %v", cache["direction"])
	}
	// first=10, last=10 → flat (or near-flat)
	if cache["direction"] != "flat" {
		t.Fatalf("V-shape ends equal: want flat, got %v (slope=%v)", cache["direction"], cache["slope"])
	}
	inf, ok := cache["inflection"].(map[string]any)
	if !ok {
		t.Fatalf("want inflection on V-shape: %#v", cache)
	}
	if inf["index"] != 2 {
		t.Fatalf("inflection index: want 2, got %v", inf["index"])
	}

	app, ok := got["app_p99_ms"].(map[string]any)
	if !ok {
		t.Fatalf("missing app_p99_ms")
	}
	if app["direction"] != "up" {
		t.Fatalf("p99 direction: want up, got %v", app["direction"])
	}
	if app["first"].(float64) != 10 || app["last"].(float64) != 50 {
		t.Fatalf("p99 first/last: %#v", app)
	}

	if _, ok := got["db_cpu_percent"]; !ok {
		t.Fatal("missing db_cpu_percent")
	}
	if _, ok := got["app_error_rate"]; !ok {
		t.Fatal("missing app_error_rate")
	}
}

func TestBuildTrajectories_EmptyAndShort(t *testing.T) {
	t.Parallel()
	if got := BuildTrajectories(nil); len(got) != 0 {
		t.Fatalf("nil: %#v", got)
	}
	hist := []DeploymentMetrics{{
		Timestamp: time.Now(),
		Components: map[string]ComponentMetrics{
			"app-server": {P99Latency: 0.01},
		},
	}}
	if got := BuildTrajectories(hist); len(got) != 0 {
		t.Fatalf("single sample should skip: %#v", got)
	}
}

func TestBuildTrajectories_MissingComponents(t *testing.T) {
	t.Parallel()
	t0 := time.Now()
	hist := []DeploymentMetrics{
		{Timestamp: t0, Components: map[string]ComponentMetrics{"app-server": {P99Latency: 0.01}}},
		{Timestamp: t0.Add(time.Second), Components: map[string]ComponentMetrics{"app-server": {P99Latency: 0.02}}},
	}
	got := BuildTrajectories(hist)
	if _, ok := got["cache_hit_rate"]; ok {
		t.Fatal("should omit cache when missing")
	}
	if _, ok := got["app_p99_ms"]; !ok {
		t.Fatal("want app_p99_ms")
	}
}
