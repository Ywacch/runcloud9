package metrics

import "github.com/ywacch/cloud9/pkg/rules"

// Visibility re-exports rules visibility constants for metrics emitters.
const (
	VisibilityStudent  = rules.VisibilityStudent
	VisibilityOperator = rules.VisibilityOperator
	VisibilityAll      = rules.VisibilityAll
)

// ValidVisibility reports whether v is empty, a supported tag, or a legacy alias.
func ValidVisibility(v string) bool {
	switch NormalizeVisibility(v) {
	case "", VisibilityStudent, VisibilityOperator, VisibilityAll:
		return true
	default:
		return false
	}
}

// NormalizeVisibility maps empty and legacy values to VisibilityAll.
func NormalizeVisibility(v string) string {
	switch v {
	case "", VisibilityAll, "both":
		return VisibilityAll
	case VisibilityStudent, VisibilityOperator:
		return v
	default:
		return v
	}
}
