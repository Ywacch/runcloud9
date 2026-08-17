package loadgen

import (
	"context"
	"log/slog"
	"sync"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// TargeterBuilder builds a vegeta targeter for the given phase-adjusted profile.
type TargeterBuilder func(ctx context.Context, profile LoadProfile, phase Phase) (vegeta.Targeter, map[string]int, error)

// AttackLoopOpts configures the shared vegeta attack loop (local generator and loadgen pod).
type AttackLoopOpts struct {
	Ctx             context.Context
	Profile         LoadProfile
	Namespace       string
	BuildTargeter   TargeterBuilder
	Stats           *LoadStats
	StatsMu         *sync.RWMutex
	Recent          *LatencyRing
	OnNaturalFinish func()
	AttackName      string
	Control         *AttackControl
}

// RunAttackLoop drives one continuous vegeta Attack. A self-rebasing pacer
// changes effective RPS live; a live targeter swaps mix/actor plans on phase
// boundaries without Stop/re-attack.
func RunAttackLoop(opts AttackLoopOpts) {
	if opts.Stats == nil || opts.BuildTargeter == nil {
		return
	}
	startedAt := time.Now()
	if opts.Stats.StartedAt != nil {
		startedAt = *opts.Stats.StartedAt
	}
	runner := NewScheduleRunner(opts.Profile.Phases, startedAt)

	control := opts.Control
	if control == nil {
		control = NewAttackControl(opts.Profile.RPS)
	}

	var cumulative vegeta.Metrics
	var window vegeta.Metrics
	var vmMu sync.Mutex

	tick1s := time.NewTicker(time.Second)
	tick5s := time.NewTicker(IntervalWindow)
	defer tick1s.Stop()
	defer tick5s.Stop()

	live := &liveTargeter{}
	pacer := newRatePacer(1)

	applyPhase := func(phase Phase, phaseIdx int, rebuild bool) int {
		targetRPS := EffectiveRPS(control.BaseRPS(), phase)
		pacer.SetTarget(targetRPS)

		var dist map[string]int
		if rebuild {
			phaseProfile := ProfileForPhase(opts.Profile, phase)
			targeter, d, err := opts.BuildTargeter(opts.Ctx, phaseProfile, phase)
			if err != nil {
				slog.Warn("loadgen phase targeter build failed", "error", err, "phase", phaseIdx)
				targeter = func(tgt *vegeta.Target) error { return nil }
				d = opts.Profile.Distribution
			}
			live.Swap(&targeterState{next: targeter, dist: d})
			dist = d
		}

		if opts.StatsMu != nil {
			opts.StatsMu.Lock()
			opts.Stats.TargetRPS = float64(targetRPS)
			opts.Stats.CurrentRPS = float64(targetRPS)
			opts.Stats.ScheduleID = opts.Profile.ScheduleID
			opts.Stats.PhaseIndex = phaseIdx
			opts.Stats.CurrentPhase = PhaseLabel(opts.Profile.ScheduleID, phase, phaseIdx)
			if len(dist) > 0 {
				opts.Stats.Distribution = copyDist(dist)
			}
			opts.StatsMu.Unlock()
		}
		return targetRPS
	}

	phase, phaseIdx := runner.Current(time.Now())
	targetRPS := applyPhase(phase, phaseIdx, true)
	lastPhaseIdx := phaseIdx

	attacker := NewVegetaAttacker(attackerRPSBudget(control.BaseRPS(), opts.Profile.Phases))
	results := attacker.Attack(live.Targeter(), pacer, opts.Profile.AttackDuration(), opts.AttackName)

	loopDone := false
	for !loopDone {
		select {
		case res, ok := <-results:
			if !ok {
				vmMu.Lock()
				if opts.StatsMu != nil {
					opts.StatsMu.Lock()
					window = CloseInterval(opts.Stats, &window, time.Now())
					cumulative.Close()
					if opts.Recent != nil {
						opts.Stats.RecentLatenciesMs = opts.Recent.Snapshot()
					}
					ApplyMetricsUpdate(opts.Stats, &cumulative, targetRPS, opts.Stats.StartedAt)
					opts.StatsMu.Unlock()
				}
				vmMu.Unlock()
				if opts.OnNaturalFinish != nil && opts.Ctx.Err() == nil {
					opts.OnNaturalFinish()
				}
				loopDone = true
				continue
			}
			vmMu.Lock()
			cumulative.Add(res)
			window.Add(res)
			if opts.Recent != nil {
				opts.Recent.Push(res.Latency.Seconds() * 1000)
			}
			vmMu.Unlock()

		case now := <-tick1s.C:
			phase, phaseIdx = runner.Current(now)
			rebuild := phaseIdx != lastPhaseIdx
			targetRPS = applyPhase(phase, phaseIdx, rebuild)
			lastPhaseIdx = phaseIdx
			vmMu.Lock()
			if opts.StatsMu != nil {
				opts.StatsMu.Lock()
				cumulative.Close()
				if opts.Recent != nil {
					opts.Stats.RecentLatenciesMs = opts.Recent.Snapshot()
				}
				ApplyMetricsUpdate(opts.Stats, &cumulative, targetRPS, opts.Stats.StartedAt)
				opts.StatsMu.Unlock()
			}
			vmMu.Unlock()

		case now := <-tick5s.C:
			vmMu.Lock()
			if opts.StatsMu != nil {
				opts.StatsMu.Lock()
				window = CloseInterval(opts.Stats, &window, now)
				opts.StatsMu.Unlock()
			}
			vmMu.Unlock()

		case <-opts.Ctx.Done():
			attacker.Stop()
			loopDone = true
		}
	}
}

// LocalTargeterBuilder resolves targets from profile for the in-process generator.
func LocalTargeterBuilder(namespace string) TargeterBuilder {
	return func(ctx context.Context, profile LoadProfile, phase Phase) (vegeta.Targeter, map[string]int, error) {
		_ = phase
		plan, err := profileToTargetPlan(ctx, namespace, profile)
		if err != nil {
			return nil, nil, err
		}
		return plan.BuildTargeter(), plan.Distribution, nil
	}
}

// PodTargeterBuilder rebuilds a targeter from pod start payload fields.
func PodTargeterBuilder(body PodStartBody) TargeterBuilder {
	return func(ctx context.Context, profile LoadProfile, phase Phase) (vegeta.Targeter, map[string]int, error) {
		_ = ctx
		_ = profile
		if body.Actor != nil {
			cfg := *body.Actor
			cfg.Distribution = EffectiveDistributionMap(body.Distribution, phase)
			cfg.ParamValues = EffectiveActorParamsMap(body.Actor.ParamValues, phase)
			return BuildTargeterFromPodActor(cfg), cfg.Distribution, nil
		}
		targets := applyPhaseToStaticTargets(body.Targets, body.Distribution, phase)
		return BuildTargeterFromResolved(targets), EffectiveDistributionMap(body.Distribution, phase), nil
	}
}

func applyPhaseToStaticTargets(targets []PodStartTarget, baseDist map[string]int, phase Phase) []ResolvedTarget {
	eff := EffectiveDistributionMap(baseDist, phase)
	out := make([]ResolvedTarget, 0, len(targets))
	for _, t := range targets {
		key := t.Key
		if key == "" {
			key = t.Method + " " + t.URL
		}
		w := t.Weight
		if nw, ok := eff[key]; ok && baseDist[key] > 0 {
			w = int(float64(w) * float64(nw) / float64(baseDist[key]))
			if w < 1 {
				w = 1
			}
		}
		out = append(out, ResolvedTarget{
			URL:    t.URL,
			Method: t.Method,
			Body:   t.Body,
			Weight: w,
			Key:    t.Key,
		})
	}
	return out
}
