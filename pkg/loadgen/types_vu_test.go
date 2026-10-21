package loadgen

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/loadgen/vu"
)

func TestValidateAndNormalizeProfile_VU(t *testing.T) {
	t.Run("nil unchanged", func(t *testing.T) {
		p, err := ValidateAndNormalizeProfile(LoadProfile{RPS: 10, Template: "social-feed"})
		if err != nil {
			t.Fatal(err)
		}
		if p.VU != nil {
			t.Fatal("expected nil vu")
		}
	})
	t.Run("51 vus", func(t *testing.T) {
		_, err := ValidateAndNormalizeProfile(LoadProfile{
			Template: "social-feed",
			RPS:      10,
			VU: &VUConfig{Behaviors: []vu.BehaviorConfig{
				{Name: "conversation_starter", Count: 51},
			}},
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("unknown behavior", func(t *testing.T) {
		_, err := ValidateAndNormalizeProfile(LoadProfile{
			Template: "social-feed",
			RPS:      10,
			VU: &VUConfig{Behaviors: []vu.BehaviorConfig{
				{Name: "not_real", Count: 1},
			}},
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("url-shortener replier", func(t *testing.T) {
		_, err := ValidateAndNormalizeProfile(LoadProfile{
			Template: "url-shortener",
			RPS:      10,
			VU: &VUConfig{Behaviors: []vu.BehaviorConfig{
				{Name: "replier", Count: 1},
			}},
		})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "url-shortener") || !strings.Contains(err.Error(), "replier") {
			t.Fatalf("err: %v", err)
		}
	})
	t.Run("vu without template", func(t *testing.T) {
		_, err := ValidateAndNormalizeProfile(LoadProfile{
			RPS: 10,
			VU:  &VUConfig{Behaviors: []vu.BehaviorConfig{{Name: "replier", Count: 1}}},
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestLocalLoadGenerator_RejectsVU(t *testing.T) {
	lg := NewLocalLoadGenerator()
	err := lg.Start(t.Context(), "ns", LoadProfile{
		Template: "social-feed",
		RPS:      10,
		VU: &VUConfig{Behaviors: []vu.BehaviorConfig{
			{Name: "conversation_starter", Count: 1},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "loadgen pod") {
		t.Fatalf("err: %v", err)
	}
}
