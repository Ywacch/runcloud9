package store

import (
	"os"
	"strconv"
	"strings"
)

const (
	// EnvInternalMaxConcurrent overrides MaxConcurrent for the internal tier when set to a positive integer.
	EnvInternalMaxConcurrent     = "INTERNAL_MAX_CONCURRENT"
	defaultInternalMaxConcurrent = 50
)

func internalMaxConcurrent() int {
	if v, ok := positiveIntFromEnv(EnvInternalMaxConcurrent); ok {
		return v
	}
	return defaultInternalMaxConcurrent
}

func positiveIntFromEnv(key string) (int, bool) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
