package metrics

import (
	"sort"
	"time"
)

const steadyStateWarmup = 30 * time.Second

// SteadyStateSummary aggregates post-warmup session metrics for A/B comparison.
// Skips the first 30s of snapshots; uses median p99/RPS/DB rate and median cache hit rate.
func SteadyStateSummary(rb *RingBuffer) map[string]any {
	empty := map[string]any{
		"steady_p99_ms":          0.0,
		"steady_rps":             0.0,
		"steady_db_query_rate":   0.0,
		"steady_cache_hit_rate":  0.0,
		"snapshot_count":         0,
	}
	if rb == nil || rb.Len() == 0 {
		return empty
	}
	all := rb.All()
	t0 := all[0].Timestamp

	var p99ms, rps, dbRate, hitRate []float64
	for i := range all {
		dm := &all[i]
		if dm.Timestamp.Sub(t0) < steadyStateWarmup {
			continue
		}
		app, ok := dm.Components["app-server"]
		if !ok {
			continue
		}
		db := dm.Components["db"]
		cache := dm.Components["cache"]
		p99ms = append(p99ms, app.P99Latency*1000)
		rps = append(rps, app.RequestRate)
		dbRate = append(dbRate, db.DbQueryRate)
		hitRate = append(hitRate, lifetimeHitRate(cache))
	}
	if len(p99ms) == 0 {
		return empty
	}
	return map[string]any{
		"steady_p99_ms":         median(p99ms),
		"steady_rps":            median(rps),
		"steady_db_query_rate":  median(dbRate),
		"steady_cache_hit_rate": median(hitRate),
		"snapshot_count":        len(p99ms),
	}
}

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}
