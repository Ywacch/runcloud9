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

func TestValidateAndNormalizeProfile_activeUsers(t *testing.T) {
	t.Parallel()
	p, err := ValidateAndNormalizeProfile(LoadProfile{RPS: 10, ActiveUsers: 3})
	if err != nil {
		t.Fatal(err)
	}
	if p.RPS != 10 || p.ActiveUsers != 3 {
		t.Fatalf("profile: rps=%d activeUsers=%d", p.RPS, p.ActiveUsers)
	}
	if _, err := ValidateAndNormalizeProfile(LoadProfile{ActiveUsers: -1}); err == nil {
		t.Fatal("expected error for negative activeUsers")
	}
	capped, err := ValidateAndNormalizeProfile(LoadProfile{RPS: 10, ActiveUsers: maxActiveUsers + 50})
	if err != nil {
		t.Fatal(err)
	}
	if capped.ActiveUsers != maxActiveUsers || capped.RPS != 10 {
		t.Fatalf("capped: rps=%d activeUsers=%d", capped.RPS, capped.ActiveUsers)
	}
}
