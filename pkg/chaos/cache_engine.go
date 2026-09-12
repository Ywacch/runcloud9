package chaos

import (
	"fmt"
	"strings"
)

// cacheFlushCommand returns the exec argv for flushing the deployment cache.
// Memcached is reserved for a future adapter (flush_all over the text protocol).
func cacheFlushCommand(engine string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case "redis":
		return []string{"redis-cli", "FLUSHALL"}, nil
	case "memcached":
		return nil, fmt.Errorf("chaos: cache_flush unsupported for engine memcached (not yet implemented)")
	default:
		if engine == "" {
			engine = "(unknown)"
		}
		return nil, fmt.Errorf("chaos: cache_flush unsupported for engine %q (supported: redis)", engine)
	}
}
