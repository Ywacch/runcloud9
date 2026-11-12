package sessionreg

import "strings"

// Enabled reports whether SESSION_REGISTRY turns on the in-memory session cache.
// Unset is off; 1, true, yes, and on enable it.
func Enabled(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
