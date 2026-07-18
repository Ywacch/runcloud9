package main

import (
	"strconv"
	"strings"
)

// parseCacheTTLSeconds interprets a CACHE_TTL env value using the deployer's
// contract: 0 means no expiry, positive values are timed expiry in seconds.
// Absent, malformed, or negative values fall back to the template default.
func parseCacheTTLSeconds(raw string, defaultSeconds int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultSeconds
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return defaultSeconds
	}
	return n
}
