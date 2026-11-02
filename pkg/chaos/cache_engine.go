package chaos

import (
	"errors"
	"fmt"
	"strings"
)

// errMemcachedFlushDial means the control plane dials Memcached.
// The memcached image has no nc, so do not exec a flush inside the pod.
var errMemcachedFlushDial = errors.New("chaos: memcached flush dials from the control plane")

// cacheFlushCommand returns the in-pod argv for a Redis flush.
// Memcached returns errMemcachedFlushDial. The caller dials flush_all.
func cacheFlushCommand(engine string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case "redis":
		return []string{"redis-cli", "FLUSHALL"}, nil
	case "memcached":
		return nil, errMemcachedFlushDial
	default:
		if engine == "" {
			engine = "(unknown)"
		}
		return nil, fmt.Errorf("chaos: cache_flush unsupported for engine %q (supported: redis, memcached)", engine)
	}
}
