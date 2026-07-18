package deployer

import (
	"strconv"
	"strings"
)

const (
	defaultWriteBehindIntervalSeconds = 30
	minWriteBehindIntervalSeconds     = 1
	maxWriteBehindIntervalSeconds     = 300
)

// normalizeWriteBehindIntervalSeconds returns WRITE_BEHIND_INTERVAL for the
// caching-proxy. Valid design values are clamped to 1–300 seconds; missing,
// malformed, or non-positive values fall back to 30.
func normalizeWriteBehindIntervalSeconds(config map[string]any) string {
	raw := strings.TrimSpace(configString(config, "writeBehindIntervalSeconds", ""))
	if raw == "" {
		return strconv.Itoa(defaultWriteBehindIntervalSeconds)
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < minWriteBehindIntervalSeconds {
		return strconv.Itoa(defaultWriteBehindIntervalSeconds)
	}
	if n > maxWriteBehindIntervalSeconds {
		return strconv.Itoa(maxWriteBehindIntervalSeconds)
	}
	return strconv.Itoa(n)
}
