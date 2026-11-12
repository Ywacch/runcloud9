package hub

import "strings"

// Enabled reports whether METRICS_HUB turns on the refresh loop.
// Unset env defaults to on; use 0, false, no, or off to disable.
func Enabled(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
