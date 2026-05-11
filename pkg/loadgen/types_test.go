package loadgen

import "testing"

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
