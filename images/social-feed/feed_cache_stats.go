package main

import "sync/atomic"

// feedCacheStats records app logical timeline hits/misses for Prometheus and /cache/stats.
type feedCacheStats struct {
	hits   *atomic.Int64
	misses *atomic.Int64
}

func (s *feedCacheStats) recordTimelineHit() {
	if s == nil || s.hits == nil {
		return
	}
	s.hits.Add(1)
	cacheHitsTotal.Inc()
}

func (s *feedCacheStats) recordTimelineMiss() {
	if s == nil || s.misses == nil {
		return
	}
	s.misses.Add(1)
	cacheMissesTotal.Inc()
}
