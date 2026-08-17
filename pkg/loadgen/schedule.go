package loadgen

import (
	"math"
	"strconv"
	"time"
)

// Phase is one step in a load schedule (YAML-authored or API-selected).
type Phase struct {
	DurationSeconds int                `json:"durationSeconds" yaml:"durationSeconds"`
	RpsMultiplier   float64            `json:"rpsMultiplier" yaml:"rpsMultiplier"`
	EndpointBoost   map[string]float64 `json:"endpointBoost,omitempty" yaml:"endpointBoost,omitempty"`
	ActorOverride   map[string]float64 `json:"actorOverride,omitempty" yaml:"actorOverride,omitempty"`
	Note            string             `json:"note,omitempty" yaml:"note"`
}

// Schedule is a named list of phases declared in template YAML.
type Schedule struct {
	ID     string  `json:"id" yaml:"id"`
	Label  string  `json:"label" yaml:"label"`
	Phases []Phase `json:"phases" yaml:"phases"`
}

type scheduleRunner struct {
	phases    []Phase
	startedAt time.Time
}

// NewScheduleRunner tracks elapsed time against a phase list. Nil or empty phases means steady load.
func NewScheduleRunner(phases []Phase, startedAt time.Time) *scheduleRunner {
	if len(phases) == 0 {
		return nil
	}
	cp := make([]Phase, len(phases))
	copy(cp, phases)
	return &scheduleRunner{phases: cp, startedAt: startedAt}
}

// Current returns the active phase and its index at now.
func (r *scheduleRunner) Current(now time.Time) (Phase, int) {
	if r == nil || len(r.phases) == 0 {
		return Phase{RpsMultiplier: 1}, 0
	}
	elapsed := now.Sub(r.startedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	var acc time.Duration
	for i, p := range r.phases {
		if p.DurationSeconds == 0 {
			return p, i
		}
		d := time.Duration(p.DurationSeconds) * time.Second
		if elapsed < acc+d {
			return p, i
		}
		acc += d
	}
	last := len(r.phases) - 1
	return r.phases[last], last
}

// PhaseAttackDuration returns how long the current vegeta Attack should run before the next phase.
// Zero means run until external stop (final steady phase or open-ended profile).
func (r *scheduleRunner) PhaseAttackDuration(now time.Time, idx int) time.Duration {
	if r == nil || idx < 0 || idx >= len(r.phases) {
		return 0
	}
	p := r.phases[idx]
	if p.DurationSeconds == 0 {
		return 0
	}
	start := r.phaseStartTime(idx)
	end := start.Add(time.Duration(p.DurationSeconds) * time.Second)
	remain := end.Sub(now)
	if remain <= 0 {
		return time.Millisecond
	}
	return remain
}

func (r *scheduleRunner) phaseStartTime(idx int) time.Time {
	var acc time.Duration
	for i := 0; i < idx && i < len(r.phases); i++ {
		if r.phases[i].DurationSeconds == 0 {
			break
		}
		acc += time.Duration(r.phases[i].DurationSeconds) * time.Second
	}
	return r.startedAt.Add(acc)
}

// PhaseLabel returns a human-readable phase name for metrics UI.
func PhaseLabel(scheduleID string, phase Phase, idx int) string {
	if phase.Note != "" {
		return phase.Note
	}
	if scheduleID != "" {
		return scheduleID + " #" + itoa(idx+1)
	}
	return "phase " + itoa(idx+1)
}

// EffectiveRPS applies the phase multiplier to the base profile RPS.
func EffectiveRPS(baseRPS int, phase Phase) int {
	mult := phase.RpsMultiplier
	if mult <= 0 {
		mult = 1
	}
	out := int(math.Round(float64(baseRPS) * mult))
	if out < 1 {
		out = 1
	}
	return out
}

// EffectiveDistributionMap merges endpoint boosts into a base distribution map.
func EffectiveDistributionMap(base map[string]int, phase Phase) map[string]int {
	out := copyIntMap(base)
	for key, factor := range phase.EndpointBoost {
		if factor <= 0 {
			continue
		}
		w, ok := out[key]
		if !ok {
			continue
		}
	 boosted := int(math.Round(float64(w) * factor))
		if boosted < 1 {
			boosted = 1
		}
		out[key] = boosted
	}
	return out
}

// EffectiveActorParamsMap merges phase actor overrides onto base param values.
func EffectiveActorParamsMap(base map[string]float64, phase Phase) map[string]float64 {
	if len(phase.ActorOverride) == 0 {
		return base
	}
	out := make(map[string]float64, len(base)+len(phase.ActorOverride))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range phase.ActorOverride {
		out[k] = v
	}
	return out
}

// PhaseNeedsTargeterRebuild reports whether endpoint or actor overrides require a new targeter.
func PhaseNeedsTargeterRebuild(phase Phase) bool {
	return len(phase.EndpointBoost) > 0 || len(phase.ActorOverride) > 0
}

// ProfileForPhase returns a copy of profile with phase distribution and actor overrides applied.
func ProfileForPhase(base LoadProfile, phase Phase) LoadProfile {
	p := base
	p.Distribution = EffectiveDistributionMap(base.Distribution, phase)
	if len(base.ActorParams) > 0 || len(phase.ActorOverride) > 0 {
		baseParams := base.ActorParams
		if baseParams == nil {
			baseParams = map[string]float64{}
		}
		p.ActorParams = EffectiveActorParamsMap(baseParams, phase)
	}
	return p
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
