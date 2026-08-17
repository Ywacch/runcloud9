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
}

// RunAttackLoop drives vegeta with optional schedule phases (re-attack per phase boundary).
func RunAttackLoop(opts AttackLoopOpts) {
	if opts.Stats == nil || opts.BuildTargeter == nil {
		return
	}
	startedAt := time.Now()
	if opts.Stats.StartedAt != nil {
		startedAt = *opts.Stats.StartedAt
	}
	runner := NewScheduleRunner(opts.Profile.Phases, startedAt)

	var cumulative vegeta.Metrics
	var window vegeta.Metrics
	var vmMu sync.Mutex

	tick1s := time.NewTicker(time.Second)
	tick5s := time.NewTicker(IntervalWindow)
	defer tick1s.Stop()
	defer tick5s.Stop()

	runPhase := func(phase Phase, phaseIdx int, attackDur time.Duration) (<-chan *vegeta.Result, *vegeta.Attacker, int) {
		phaseProfile := ProfileForPhase(opts.Profile, phase)
		targetRPS := EffectiveRPS(opts.Profile.RPS, phase)
		targeter, dist, err := opts.BuildTargeter(opts.Ctx, phaseProfile, phase)
		if err != nil {
			slog.Warn("loadgen phase targeter build failed", "error", err, "phase", phaseIdx)
			targeter = func(tgt *vegeta.Target) error { return nil }
			dist = opts.Profile.Distribution
		}
		attacker := NewVegetaAttacker(targetRPS)
		rate := vegeta.Rate{Freq: targetRPS, Per: time.Second}
		if opts.Profile.DurationSeconds > 0 && attackDur == 0 {
			attackDur = opts.Profile.AttackDuration()
		}
		results := attacker.Attack(targeter, rate, attackDur, opts.AttackName)

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
		return results, attacker, targetRPS
	}

	phase, phaseIdx := runner.Current(time.Now())
	attackDur := runner.PhaseAttackDuration(time.Now(), phaseIdx)
	results, attacker, targetRPS := runPhase(phase, phaseIdx, attackDur)
	lastPhaseIdx := phaseIdx

	loopDone := false
	for !loopDone {
		select {
		case res, ok := <-results:
			if !ok {
				if opts.Ctx.Err() != nil {
					loopDone = true
					continue
				}
				now := time.Now()
				nextPhase, nextIdx := runner.Current(now)
				if runner != nil && nextIdx != lastPhaseIdx {
					phase, phaseIdx = nextPhase, nextIdx
					attackDur = runner.PhaseAttackDuration(now, phaseIdx)
					results, attacker, targetRPS = runPhase(phase, phaseIdx, attackDur)
					lastPhaseIdx = phaseIdx
					continue
				}
				vmMu.Lock()
				if opts.StatsMu != nil {
					opts.StatsMu.Lock()
					window = CloseInterval(opts.Stats, &window, now)
					cumulative.Close()
					if opts.Recent != nil {
						opts.Stats.RecentLatenciesMs = opts.Recent.Snapshot()
					}
					ApplyMetricsUpdate(opts.Stats, &cumulative, targetRPS, opts.Stats.StartedAt)
					opts.StatsMu.Unlock()
				}
				vmMu.Unlock()
				if opts.OnNaturalFinish != nil {
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
			if runner != nil && phaseIdx != lastPhaseIdx {
				attacker.Stop()
				attackDur = runner.PhaseAttackDuration(now, phaseIdx)
				results, attacker, targetRPS = runPhase(phase, phaseIdx, attackDur)
				lastPhaseIdx = phaseIdx
				continue
			}
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
		_ = phase
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
