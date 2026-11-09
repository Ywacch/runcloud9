package hub

import "strings"

// Enabled reports whether METRICS_HUB turns on the refresh loop.
func Enabled(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
