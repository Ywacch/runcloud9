package kvcache

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// cacheOpBuckets cover a cache round trip from 100µs through the client timeout.
var cacheOpBuckets = []float64{
	0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1,
}

var cacheOpDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "cache_op_duration_seconds",
		Help:    "Round trip time for a kvcache Get, Set, or Delete.",
		Buckets: cacheOpBuckets,
	},
	[]string{"engine", "op", "result"},
)

var cacheOpObservers = map[string]prometheus.Observer{}

func init() {
	engines := []string{"redis", "memcached"}
	for _, engine := range engines {
		for _, op := range []string{"get", "set", "delete"} {
			results := []string{"ok", "error", "timeout"}
			if op == "get" {
				results = []string{"hit", "miss", "error", "timeout"}
			}
			for _, result := range results {
				cacheOpObservers[observerKey(engine, op, result)] = cacheOpDuration.WithLabelValues(engine, op, result)
			}
		}
	}
}

func observerKey(engine, op, result string) string {
	return engine + "\x00" + op + "\x00" + result
}

func observeCacheOp(engine, op, result string, start time.Time) {
	obs, ok := cacheOpObservers[observerKey(engine, op, result)]
	if !ok {
		return
	}
	obs.Observe(time.Since(start).Seconds())
}

func cacheOpResult(op string, hit bool, err error) string {
	if err == nil {
		if op == "get" {
			if hit {
				return "hit"
			}
			return "miss"
		}
		return "ok"
	}
	if isCacheTimeout(err) {
		return "timeout"
	}
	return "error"
}

func isCacheTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
