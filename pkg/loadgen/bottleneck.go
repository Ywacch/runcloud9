package loadgen

import "time"

// DetectionMayBeIncomplete reports whether the generator could not keep up, so
// observer counts undercount. LG-8 extends this with loadgen pod CPU.
func DetectionMayBeIncomplete(s LoadStats) bool {
	if !s.Active || s.TargetRPS <= 0 {
		return false
	}
	if s.StartedAt == nil {
		return false
	}
	if time.Since(*s.StartedAt) < 15*time.Second {
		return false
	}
	return s.ActualRPS < 0.9*s.TargetRPS
}
