package templates

// LoadgenSchedule declares a phased load curve for a template.
type LoadgenSchedule struct {
	ID     string             `yaml:"id" json:"id"`
	Label  string             `yaml:"label" json:"label"`
	Phases []LoadgenPhaseSpec `yaml:"phases" json:"phases"`
}

// LoadgenPhaseSpec is one phase in a template schedule.
type LoadgenPhaseSpec struct {
	DurationSeconds int                `yaml:"durationSeconds" json:"durationSeconds"`
	RpsMultiplier   float64            `yaml:"rpsMultiplier" json:"rpsMultiplier"`
	EndpointBoost   map[string]float64 `yaml:"endpointBoost,omitempty" json:"endpointBoost,omitempty"`
	ActorOverride   map[string]float64 `yaml:"actorOverride,omitempty" json:"actorOverride,omitempty"`
	Note            string             `yaml:"note,omitempty" json:"note,omitempty"`
}

type loadgenScheduleDef struct {
	ID     string            `yaml:"id" json:"id"`
	Label  string            `yaml:"label" json:"label"`
	Phases []loadgenPhaseDef `yaml:"phases" json:"phases"`
}

type loadgenPhaseDef struct {
	DurationSeconds int                `yaml:"durationSeconds" json:"durationSeconds"`
	RpsMultiplier   float64            `yaml:"rpsMultiplier" json:"rpsMultiplier"`
	EndpointBoost   map[string]float64 `yaml:"endpointBoost" json:"endpointBoost,omitempty"`
	ActorOverride   map[string]float64 `yaml:"actorOverride" json:"actorOverride,omitempty"`
	Note            string             `yaml:"note" json:"note,omitempty"`
}

// GetLoadgenSchedule returns one schedule by id for templateID.
func GetLoadgenSchedule(templateID, scheduleID string) (LoadgenSchedule, bool) {
	schedules, ok := LoadgenSchedules(templateID)
	if !ok {
		return LoadgenSchedule{}, false
	}
	for _, s := range schedules {
		if s.ID == scheduleID {
			return s, true
		}
	}
	return LoadgenSchedule{}, false
}

// LoadgenSchedules returns all schedules declared for templateID.
func LoadgenSchedules(templateID string) ([]LoadgenSchedule, bool) {
	t := GetTemplate(templateID)
	if t == nil || t.Loadgen == nil || len(t.Loadgen.Schedules) == 0 {
		return nil, false
	}
	out := make([]LoadgenSchedule, len(t.Loadgen.Schedules))
	copy(out, t.Loadgen.Schedules)
	return out, true
}

func loadgenSchedulesFromDef(defs []loadgenScheduleDef) []LoadgenSchedule {
	if len(defs) == 0 {
		return nil
	}
	out := make([]LoadgenSchedule, 0, len(defs))
	for _, d := range defs {
		sched := LoadgenSchedule{
			ID:     d.ID,
			Label:  d.Label,
			Phases: make([]LoadgenPhaseSpec, 0, len(d.Phases)),
		}
		for _, p := range d.Phases {
			phase := LoadgenPhaseSpec{
				DurationSeconds: p.DurationSeconds,
				RpsMultiplier:   p.RpsMultiplier,
				Note:            p.Note,
			}
			if len(p.EndpointBoost) > 0 {
				phase.EndpointBoost = copyFloatMap(p.EndpointBoost)
			}
			if len(p.ActorOverride) > 0 {
				phase.ActorOverride = copyFloatMap(p.ActorOverride)
			}
			if phase.RpsMultiplier <= 0 {
				phase.RpsMultiplier = 1
			}
			sched.Phases = append(sched.Phases, phase)
		}
		out = append(out, sched)
	}
	return out
}

func copyFloatMap(m map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
