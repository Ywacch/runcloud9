package timeline

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// OpenRedis builds an instrumented Redis timeline store when engineEnv is empty or "redis".
func OpenRedis(client redis.UniversalClient, engineEnv string, opts RedisOptions, reg prometheus.Registerer) (Store, error) {
	switch strings.ToLower(strings.TrimSpace(engineEnv)) {
	case "", "redis":
		if reg == nil {
			reg = prometheus.DefaultRegisterer
		}
		return Instrumented(NewRedis(client, opts), reg), nil
	default:
		return nil, fmt.Errorf("timeline: unsupported TIMELINE_ENGINE %q", strings.TrimSpace(engineEnv))
	}
}
