package loadgen

import (
	"testing"
	"time"
)

func TestDetectionMayBeIncomplete(t *testing.T) {
	started := time.Now().Add(-20 * time.Second)
	t.Run("inactive", func(t *testing.T) {
		if DetectionMayBeIncomplete(LoadStats{Active: false, TargetRPS: 50, ActualRPS: 10, StartedAt: &started}) {
			t.Fatal("expected false")
		}
	})
	t.Run("warmup", func(t *testing.T) {
		warm := time.Now().Add(-5 * time.Second)
		if DetectionMayBeIncomplete(LoadStats{Active: true, TargetRPS: 50, ActualRPS: 10, StartedAt: &warm}) {
			t.Fatal("expected false during warmup")
		}
	})
	t.Run("85pct", func(t *testing.T) {
		if !DetectionMayBeIncomplete(LoadStats{Active: true, TargetRPS: 100, ActualRPS: 85, StartedAt: &started}) {
			t.Fatal("expected true at 85%")
		}
	})
	t.Run("95pct", func(t *testing.T) {
		if DetectionMayBeIncomplete(LoadStats{Active: true, TargetRPS: 100, ActualRPS: 95, StartedAt: &started}) {
			t.Fatal("expected false at 95%")
		}
	})
}
