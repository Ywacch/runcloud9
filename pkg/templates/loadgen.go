package templates

// LoadgenEndpointSpec describes one HTTP endpoint the load generator may target.
type LoadgenEndpointSpec struct {
	Method       string `yaml:"method" json:"method"`
	PathTemplate string `yaml:"pathTemplate" json:"pathTemplate"`
	ParamName    string `yaml:"paramName" json:"paramName"`
	PoolEndpoint string `yaml:"poolEndpoint" json:"poolEndpoint"`
	PoolField    string `yaml:"poolField" json:"poolField"`
	BodyTemplate string `yaml:"bodyTemplate" json:"bodyTemplate"`
	Weight       int    `yaml:"weight" json:"weight"`
}

// LoadgenSpec is the template-authored load generator configuration.
type LoadgenSpec struct {
	DefaultRPS int                   `yaml:"defaultRps" json:"defaultRps,omitempty"`
	Endpoints  []LoadgenEndpointSpec `yaml:"endpoints" json:"endpoints,omitempty"`
	Params     []LoadgenActorParam   `yaml:"params" json:"params,omitempty"`
	Schedules  []LoadgenSchedule     `yaml:"schedules" json:"schedules,omitempty"`
}

type loadgenEndpointDef struct {
	Method       string `yaml:"method"`
	PathTemplate string `yaml:"pathTemplate"`
	ParamName    string `yaml:"paramName"`
	PoolEndpoint string `yaml:"poolEndpoint"`
	PoolField    string `yaml:"poolField"`
	BodyTemplate string `yaml:"bodyTemplate"`
	Weight       int    `yaml:"weight"`
}

type loadgenDef struct {
	DefaultRps int                    `yaml:"defaultRps"`
	Endpoints  []loadgenEndpointDef   `yaml:"endpoints"`
	Params     []loadgenActorParamDef `yaml:"params"`
	Schedules  []loadgenScheduleDef   `yaml:"schedules"`
}

// LoadgenEndpoints returns a deep copy of endpoint specs for templateID.
func LoadgenEndpoints(templateID string) ([]LoadgenEndpointSpec, bool) {
	t := GetTemplate(templateID)
	if t == nil || t.Loadgen == nil || len(t.Loadgen.Endpoints) == 0 {
		return nil, false
	}
	out := make([]LoadgenEndpointSpec, len(t.Loadgen.Endpoints))
	copy(out, t.Loadgen.Endpoints)
	return out, true
}

// HasLoadgenEndpoints reports whether templateID has loadgen endpoint definitions.
func HasLoadgenEndpoints(templateID string) bool {
	_, ok := LoadgenEndpoints(templateID)
	return ok
}

// LoadgenParams returns actor param definitions for templateID.
func LoadgenParams(templateID string) ([]LoadgenActorParam, bool) {
	t := GetTemplate(templateID)
	if t == nil || t.Loadgen == nil || len(t.Loadgen.Params) == 0 {
		return nil, false
	}
	out := make([]LoadgenActorParam, len(t.Loadgen.Params))
	copy(out, t.Loadgen.Params)
	return out, true
}

// LoadgenActorParamView is the public JSON projection of an actor param (includes tier as data).
type LoadgenActorParamView struct {
	Key     string             `json:"key"`
	Type    string             `json:"type"`
	Default float64            `json:"default"`
	Min     *float64           `json:"min,omitempty"`
	Max     *float64           `json:"max,omitempty"`
	Pools   []LoadgenActorPool `json:"pools,omitempty"`
	ApplyTo []string           `json:"applyTo,omitempty"`
	Tier    string             `json:"tier,omitempty"`
}

// LoadgenEndpointView is one endpoint row in the Load UI contract (no bodyTemplate).
type LoadgenEndpointView struct {
	Method       string `json:"method"`
	PathTemplate string `json:"pathTemplate"`
	Weight       int    `json:"weight"`
}

// LoadgenConfigView is the Load UI contract for schedules and actor params.
type LoadgenConfigView struct {
	DefaultRPS int                     `json:"defaultRps,omitempty"`
	Endpoints  []LoadgenEndpointView   `json:"endpoints,omitempty"`
	Params     []LoadgenActorParamView `json:"params,omitempty"`
	Schedules  []LoadgenSchedule       `json:"schedules,omitempty"`
}

// LoadgenConfig returns the projected loadgen UI config for templateID.
func LoadgenConfig(templateID string) (LoadgenConfigView, bool) {
	t := GetTemplate(templateID)
	if t == nil || t.Loadgen == nil {
		return LoadgenConfigView{}, false
	}
	lg := t.Loadgen
	view := LoadgenConfigView{
		DefaultRPS: lg.DefaultRPS,
		Endpoints:  make([]LoadgenEndpointView, 0, len(lg.Endpoints)),
		Params:     make([]LoadgenActorParamView, 0, len(lg.Params)),
	}
	for _, ep := range lg.Endpoints {
		view.Endpoints = append(view.Endpoints, LoadgenEndpointView{
			Method:       ep.Method,
			PathTemplate: ep.PathTemplate,
			Weight:       ep.Weight,
		})
	}
	schedules, ok := LoadgenSchedules(templateID)
	if ok {
		view.Schedules = schedules
	}
	for _, p := range lg.Params {
		view.Params = append(view.Params, LoadgenActorParamView{
			Key:     p.Key,
			Type:    p.Type,
			Default: p.Default,
			Min:     p.Min,
			Max:     p.Max,
			Pools:   append([]LoadgenActorPool(nil), p.Pools...),
			ApplyTo: append([]string(nil), p.ApplyTo...),
			Tier:    p.Tier,
		})
	}
	return view, true
}

func loadgenSpecFromDef(def *loadgenDef) *LoadgenSpec {
	if def == nil {
		return nil
	}
	spec := &LoadgenSpec{
		DefaultRPS: def.DefaultRps,
		Endpoints:  make([]LoadgenEndpointSpec, 0, len(def.Endpoints)),
		Params:     make([]LoadgenActorParam, 0, len(def.Params)),
		Schedules:  loadgenSchedulesFromDef(def.Schedules),
	}
	for _, ep := range def.Endpoints {
		spec.Endpoints = append(spec.Endpoints, LoadgenEndpointSpec(ep))
	}
	for _, p := range def.Params {
		param := LoadgenActorParam{
			Key:     p.Key,
			Type:    p.Type,
			Default: p.Default,
			Min:     p.Min,
			Max:     p.Max,
			ApplyTo: append([]string(nil), p.ApplyTo...),
			Tier:    p.Tier,
		}
		for _, pool := range p.Pools {
			param.Pools = append(param.Pools, LoadgenActorPool(pool))
		}
		spec.Params = append(spec.Params, param)
	}
	return spec
}
