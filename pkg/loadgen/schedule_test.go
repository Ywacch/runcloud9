package loadgen

import (
	"testing"
	"time"
)

func TestScheduleRunner_phaseBoundaries(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	phases := []Phase{
		{DurationSeconds: 10, RpsMultiplier: 1, Note: "warmup"},
		{DurationSeconds: 20, RpsMultiplier: 2, Note: "spike"},
		{DurationSeconds: 0, RpsMultiplier: 1, Note: "steady"},
	}
	r := NewScheduleRunner(phases, start)

	p, idx := r.Current(start.Add(5 * time.Second))
	if idx != 0 || p.Note != "warmup" {
		t.Fatalf("at 5s: got idx=%d note=%q", idx, p.Note)
	}

	p, idx = r.Current(start.Add(15 * time.Second))
	if idx != 1 || p.Note != "spike" {
		t.Fatalf("at 15s: got idx=%d note=%q", idx, p.Note)
	}

	p, idx = r.Current(start.Add(35 * time.Second))
	if idx != 2 || p.Note != "steady" {
		t.Fatalf("at 35s: got idx=%d note=%q", idx, p.Note)
	}

	p, idx = r.Current(start.Add(24 * time.Hour))
	if idx != 2 || p.Note != "steady" {
		t.Fatalf("final phase sticks: got idx=%d note=%q", idx, p.Note)
	}
}

func TestEffectiveRPS_multiplier(t *testing.T) {
	t.Parallel()
	if got := EffectiveRPS(25, Phase{RpsMultiplier: 4}); got != 100 {
		t.Fatalf("got %d want 100", got)
	}
	if got := EffectiveRPS(25, Phase{RpsMultiplier: 0}); got != 25 {
		t.Fatalf("zero multiplier defaults to 1: got %d", got)
	}
}

func TestEffectiveDistributionMap_endpointBoost(t *testing.T) {
	t.Parallel()
	base := map[string]int{"POST /post": 30, "GET /feed/{userId}": 60}
	phase := Phase{EndpointBoost: map[string]float64{"POST /post": 2.0}}
	got := EffectiveDistributionMap(base, phase)
	if got["POST /post"] != 60 || got["GET /feed/{userId}"] != 60 {
		t.Fatalf("got %+v", got)
	}
}

func TestEffectiveActorParamsMap_override(t *testing.T) {
	t.Parallel()
	base := map[string]float64{"celebrityPostPct": 5, "readAfterPostBoost": 1}
	phase := Phase{ActorOverride: map[string]float64{"celebrityPostPct": 90}}
	got := EffectiveActorParamsMap(base, phase)
	if got["celebrityPostPct"] != 90 || got["readAfterPostBoost"] != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestPhaseAttackDuration_timedPhase(t *testing.T) {
	t.Parallel()
	start := time.Now()
	r := NewScheduleRunner([]Phase{{DurationSeconds: 30, RpsMultiplier: 1}}, start)
	d := r.PhaseAttackDuration(start.Add(5*time.Second), 0)
	if d < 24*time.Second || d > 26*time.Second {
		t.Fatalf("remaining duration: got %v", d)
	}
}

func TestValidateAndNormalizeProfile_unknownSchedule(t *testing.T) {
	t.Parallel()
	_, err := ValidateAndNormalizeProfile(LoadProfile{
		Template:   "social-feed",
		RPS:        10,
		ScheduleID: "no-such-schedule",
	})
	if err == nil {
		t.Fatal("expected unknown schedule error")
	}
}

func TestValidateAndNormalizeProfile_resolvesSchedule(t *testing.T) {
	t.Parallel()
	p, err := ValidateAndNormalizeProfile(LoadProfile{
		Template:   "social-feed",
		RPS:        10,
		ScheduleID: "steady",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Phases) != 1 || p.Phases[0].RpsMultiplier != 1 {
		t.Fatalf("phases: %+v", p.Phases)
	}
}
