package timeline

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// Open selects a timeline Store from TIMELINE_ENGINE and wraps it with Instrumented metrics.
func Open(
	engineEnv string,
	reg prometheus.Registerer,
	redisClient redis.UniversalClient,
	redisOpts RedisOptions,
	pgWriter PGWriter,
	pgReader PGReader,
	pgOpts PostgresOptions,
) (Store, error) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	switch strings.ToLower(strings.TrimSpace(engineEnv)) {
	case "", "redis":
		if redisClient == nil {
			return nil, fmt.Errorf("timeline: redis client required for engine %q", strings.TrimSpace(engineEnv))
		}
		return OpenRedis(redisClient, engineEnv, redisOpts, reg)
	case "postgresql", "postgres":
		if pgWriter == nil {
			return nil, fmt.Errorf("timeline: postgres writer required for engine %q", engineEnv)
		}
		if pgReader == nil {
			var err error
			pgReader, err = coalescePGReader(pgWriter, nil)
			if err != nil {
				return nil, err
			}
		}
		base, err := NewPostgres(pgWriter, pgReader, pgOpts)
		if err != nil {
			return nil, err
		}
		return Instrumented(base, reg), nil
	default:
		return nil, fmt.Errorf("timeline: unsupported TIMELINE_ENGINE %q", strings.TrimSpace(engineEnv))
	}
}
