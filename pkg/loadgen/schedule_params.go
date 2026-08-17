package loadgen

import (
	"fmt"

	"github.com/ywacch/cloud9/pkg/templates"
)

func phasesFromTemplateSchedule(templateID, scheduleID string) ([]Phase, error) {
	sched, ok := templates.GetLoadgenSchedule(templateID, scheduleID)
	if !ok {
		return nil, fmt.Errorf("loadgen: unknown schedule %q for template %q", scheduleID, templateID)
	}
	if len(sched.Phases) == 0 {
		return nil, fmt.Errorf("loadgen: schedule %q has no phases", scheduleID)
	}
	out := make([]Phase, len(sched.Phases))
	for i, p := range sched.Phases {
		out[i] = phaseFromTemplate(p)
	}
	return out, nil
}

func phaseFromTemplate(p templates.LoadgenPhaseSpec) Phase {
	ph := Phase{
		DurationSeconds: p.DurationSeconds,
		RpsMultiplier:   p.RpsMultiplier,
		Note:            p.Note,
	}
	if len(p.EndpointBoost) > 0 {
		ph.EndpointBoost = copyFloat64Map(p.EndpointBoost)
	}
	if len(p.ActorOverride) > 0 {
		ph.ActorOverride = copyFloat64Map(p.ActorOverride)
	}
	if ph.RpsMultiplier <= 0 {
		ph.RpsMultiplier = 1
	}
	return ph
}

func resolveSchedulePhases(p LoadProfile) ([]Phase, error) {
	if p.ScheduleID == "" {
		return nil, nil
	}
	if p.Template == "" {
		return nil, fmt.Errorf("loadgen: schedule requires template")
	}
	phases, err := phasesFromTemplateSchedule(p.Template, p.ScheduleID)
	if err != nil {
		return nil, err
	}
	if len(p.Phases) > 0 {
		return p.Phases, nil
	}
	return phases, nil
}

func validatePhaseActorOverrides(templateID string, phases []Phase) error {
	defs, hasParams := actorParamsFromTemplate(templateID)
	if !hasParams {
		for _, ph := range phases {
			if len(ph.ActorOverride) > 0 {
				return fmt.Errorf("loadgen: template %q does not declare actor params", templateID)
			}
		}
		return nil
	}
	byKey := make(map[string]ActorParam, len(defs))
	for _, d := range defs {
		byKey[d.Key] = d
	}
	for _, ph := range phases {
		for key, value := range ph.ActorOverride {
			def, ok := byKey[key]
			if !ok {
				return fmt.Errorf("loadgen: unknown actor param %q in schedule phase", key)
			}
			if err := validateActorValue(def, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyFloat64Map(m map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
