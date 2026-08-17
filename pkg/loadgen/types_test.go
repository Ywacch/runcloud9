package loadgen

import (
	"strings"
	"testing"
)

func TestNormalizeProfile_LoadgenDefaultRPSFromTemplate(t *testing.T) {
	p := NormalizeProfile(LoadProfile{RPS: 0, Template: "social-feed-pull"})
	if p.RPS != 25 {
		t.Fatalf("social-feed-pull default RPS: got %d want 25", p.RPS)
	}

	p = NormalizeProfile(LoadProfile{RPS: 0, Template: "url-shortener"})
	if p.RPS != 50 {
		t.Fatalf("unknown template fallback RPS: got %d want 50", p.RPS)
	}

	p = NormalizeProfile(LoadProfile{RPS: 40, Template: "social-feed-pull"})
	if p.RPS != 40 {
		t.Fatalf("explicit RPS preserved: got %d want 40", p.RPS)
	}
}

func TestValidateAndNormalizeProfile_actorParams(t *testing.T) {
	t.Parallel()

	p, err := ValidateAndNormalizeProfile(LoadProfile{Template: "social-feed", RPS: 10})
	if err != nil {
		t.Fatal(err)
	}
	if p.ActorParams["celebrityPostPct"] != 5 {
		t.Fatalf("defaults: %+v", p.ActorParams)
	}

	_, err = ValidateAndNormalizeProfile(LoadProfile{
		Template:    "url-shortener",
		RPS:         10,
		ActorParams: map[string]float64{"celebrityPostPct": 1},
	})
	if err == nil || !strings.Contains(err.Error(), "does not declare actor params") {
		t.Fatalf("expected no-params error, got %v", err)
	}
}
