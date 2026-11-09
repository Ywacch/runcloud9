package sidework

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// SlotsFromEnv reads SIDEWORK_SLOTS (default 32).
func SlotsFromEnv() int {
	return positiveIntEnv(os.Getenv("SIDEWORK_SLOTS"), 32)
}

// CadenceFromEnv reads SIDEWORK_CADENCE (default 6s).
func CadenceFromEnv() time.Duration {
	return durationEnv(os.Getenv("SIDEWORK_CADENCE"), 6*time.Second)
}

func positiveIntEnv(raw string, def int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return def
	}
	return n
}

func durationEnv(raw string, def time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return def
	}
	return d
}
