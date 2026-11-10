package store

import (
	"testing"
)

func TestInternalMaxConcurrentFromEnv(t *testing.T) {
	t.Setenv(EnvInternalMaxConcurrent, "100")
	if got := GetTierLimits(TierInternal).MaxConcurrent; got != 100 {
		t.Fatalf("MaxConcurrent = %d, want 100", got)
	}

	t.Setenv(EnvInternalMaxConcurrent, "0")
	if got := GetTierLimits(TierInternal).MaxConcurrent; got != defaultInternalMaxConcurrent {
		t.Fatalf("invalid env MaxConcurrent = %d, want default %d", got, defaultInternalMaxConcurrent)
	}

	t.Setenv(EnvInternalMaxConcurrent, "")
	if got := GetTierLimits(TierInternal).MaxConcurrent; got != defaultInternalMaxConcurrent {
		t.Fatalf("unset env MaxConcurrent = %d, want %d", got, defaultInternalMaxConcurrent)
	}
}
