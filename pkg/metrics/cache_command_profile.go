package metrics

// PromRateWindowLabel documents the PromQL range used for live cache command rates.
const PromRateWindowLabel = "10s"

const minAppRequestRateForCacheRatio = 1.0

// ReadPathCmdCounts is the expected Redis command count per app read (hit vs miss).
type ReadPathCmdCounts struct {
	HitRedisOps  float64
	MissRedisOps float64
}

// ReadPathCmdCountsFor returns hit/miss Redis op counts for the read path model.
func ReadPathCmdCountsFor(cache ComponentMetrics, template string) ReadPathCmdCounts {
	if usesProxyReadPath(cache) {
		return ReadPathCmdCounts{HitRedisOps: 1, MissRedisOps: 2}
	}
	_ = template
	return ReadPathCmdCounts{HitRedisOps: 1, MissRedisOps: 3}
}

func usesProxyReadPath(cache ComponentMetrics) bool {
	switch cache.CacheStrategy {
	case "write-through", "write-behind":
		return true
	}
	return cache.CacheMetricMode == "layered"
}

// RequestPathRedisOpsPerSec sums allowlisted redis command rates excluding SCAN (flusher).
func RequestPathRedisOpsPerSec(rates map[string]float64) float64 {
	if len(rates) == 0 {
		return 0
	}
	var sum float64
	for cmd, rate := range rates {
		if cmd == "scan" {
			continue
		}
		sum += rate
	}
	return sum
}

// ExpectedReadPathRedisOpsPerSec models request-path Redis ops/s from app hit/miss rates.
func ExpectedReadPathRedisOpsPerSec(app ComponentMetrics, cache ComponentMetrics, template string) float64 {
	hits := app.CacheHits
	misses := app.CacheMisses
	if hits <= 0 && misses <= 0 {
		return 0
	}
	counts := ReadPathCmdCountsFor(cache, template)
	return hits*counts.HitRedisOps + misses*counts.MissRedisOps
}

func appendRedisCommandSignals(m map[string]any, cm ComponentMetrics) {
	if cm.Component != "cache" {
		return
	}
	writeRedisCommandSignals(m, cm.RedisOpsPerSec, cm.RedisCmdRates)
}

func appendRedisCommandSignalsToLayer(m map[string]any, layer ComponentLayerMetrics) {
	if layer.Engine != layerEngineRedis && len(layer.RedisCmdRates) == 0 && layer.RedisOpsPerSec <= 0 {
		return
	}
	writeRedisCommandSignals(m, layer.RedisOpsPerSec, layer.RedisCmdRates)
}

func writeRedisCommandSignals(m map[string]any, opsPerSec float64, rates map[string]float64) {
	if len(rates) > 0 {
		opsPerSec = RequestPathRedisOpsPerSec(rates)
	}
	if opsPerSec > 0 {
		setFloat(m, "redisOpsPerSec", opsPerSec)
	}
	for cmd, rate := range rates {
		setFloat(m, "redisCmd."+cmd, rate)
	}
}
