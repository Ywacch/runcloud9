package postgres

import (
	"os"
	"strings"
)

// ReadReplicaHostsFromEnv parses READ_REPLICA_HOSTS (comma-separated ordinals),
// falling back to fallbackHost when the list env is unset or empty.
func ReadReplicaHostsFromEnv(fallbackHost string) []string {
	if raw := os.Getenv("READ_REPLICA_HOSTS"); raw != "" {
		var hosts []string
		for _, part := range strings.Split(raw, ",") {
			if h := strings.TrimSpace(part); h != "" {
				hosts = append(hosts, h)
			}
		}
		if len(hosts) > 0 {
			return hosts
		}
	}
	if h := strings.TrimSpace(fallbackHost); h != "" {
		return []string{h}
	}
	return nil
}
