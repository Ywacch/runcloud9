package main

import (
	"os"
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

// cacheTTLSecondsFromEnv resolves the TTL from the first env var that is set.
// Presence wins before validity so a template-specific override (e.g.
// CATALOG_CACHE_TTL) always shadows the generic CACHE_TTL.
func cacheTTLSecondsFromEnv(defaultSeconds int, keys ...string) int {
	for _, key := range keys {
		if v := os.Getenv(key); v != "" {
			return parseCacheTTLSeconds(v, defaultSeconds)
		}
	}
	return defaultSeconds
}
