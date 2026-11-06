package clusterstate

import "strings"

// Enabled reports whether shared informers should run.
// It is on only when rawEnv is "1" or "true" (any case).
func Enabled(rawEnv string) bool {
	switch strings.ToLower(strings.TrimSpace(rawEnv)) {
	case "1", "true":
		return true
	default:
		return false
	}
}
