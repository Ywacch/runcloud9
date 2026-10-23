package templates

import "fmt"

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

// LoadgenIdentity names the endpoint param that carries the acting user.
type LoadgenIdentity struct {
	Param string `yaml:"param" json:"param"`
}

// LoadgenSpec is the template-authored load generator configuration.
type LoadgenSpec struct {
	DefaultRPS int                   `yaml:"defaultRps" json:"defaultRps,omitempty"`
	Endpoints  []LoadgenEndpointSpec `yaml:"endpoints" json:"endpoints,omitempty"`
	Params     []LoadgenActorParam   `yaml:"params" json:"params,omitempty"`
	Schedules  []LoadgenSchedule     `yaml:"schedules" json:"schedules,omitempty"`
	VU         *LoadgenVUSpec        `yaml:"vu" json:"vu,omitempty"`
	Identity   *LoadgenIdentity      `yaml:"identity" json:"identity,omitempty"`
}

// LoadgenVUSpec declares identity pool and named HTTP actions for the VU lane.
type LoadgenVUSpec struct {
	Identity LoadgenVUIdentity          `yaml:"identity" json:"identity"`
	Actions  map[string]LoadgenVUAction `yaml:"actions" json:"actions,omitempty"`
}

// LoadgenVUIdentity selects the user identity pool for VU behaviors.
type LoadgenVUIdentity struct {
	PoolEndpoint string `yaml:"poolEndpoint" json:"poolEndpoint"`
	PoolField    string `yaml:"poolField" json:"poolField"`
}

// LoadgenVUAction is one HTTP call template for the VU lane.
type LoadgenVUAction struct {
	Method  string `yaml:"method" json:"method"`
	Path    string `yaml:"path" json:"path"`
	Body    string `yaml:"body" json:"body,omitempty"`
	IDField string `yaml:"idField" json:"idField,omitempty"`
}

type loadgenEndpointDef struct {
	Method       string `yaml:"method" json:"method"`
	PathTemplate string `yaml:"pathTemplate" json:"pathTemplate"`
	ParamName    string `yaml:"paramName" json:"paramName"`
	PoolEndpoint string `yaml:"poolEndpoint" json:"poolEndpoint"`
	PoolField    string `yaml:"poolField" json:"poolField"`
	BodyTemplate string `yaml:"bodyTemplate" json:"bodyTemplate"`
	Weight       int    `yaml:"weight" json:"weight"`
}

type loadgenDef struct {
	DefaultRps int                    `yaml:"defaultRps" json:"defaultRps"`
	Endpoints  []loadgenEndpointDef   `yaml:"endpoints" json:"endpoints"`
	Params     []loadgenActorParamDef `yaml:"params" json:"params"`
	Schedules  []loadgenScheduleDef   `yaml:"schedules" json:"schedules"`
	VU         *loadgenVUSpecDef      `yaml:"vu" json:"vu"`
	Identity   *loadgenIdentityDef    `yaml:"identity" json:"identity"`
}

type loadgenIdentityDef struct {
	Param string `yaml:"param" json:"param"`
}

type loadgenVUSpecDef struct {
	Identity loadgenVUIdentityDef          `yaml:"identity" json:"identity"`
	Actions  map[string]loadgenVUActionDef `yaml:"actions" json:"actions"`
}

type loadgenVUIdentityDef struct {
	PoolEndpoint string `yaml:"poolEndpoint" json:"poolEndpoint"`
	PoolField    string `yaml:"poolField" json:"poolField"`
}

type loadgenVUActionDef struct {
	Method  string `yaml:"method" json:"method"`
	Path    string `yaml:"path" json:"path"`
	Body    string `yaml:"body" json:"body"`
	IDField string `yaml:"idField" json:"idField"`
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
	Identity   *LoadgenIdentity        `json:"identity,omitempty"`
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
	if lg.Identity != nil && lg.Identity.Param != "" {
		view.Identity = &LoadgenIdentity{Param: lg.Identity.Param}
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
	if def.VU != nil {
		spec.VU = loadgenVUSpecFromDef(def.VU)
	}
	if def.Identity != nil && def.Identity.Param != "" {
		spec.Identity = &LoadgenIdentity{Param: def.Identity.Param}
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

func loadgenVUSpecFromDef(def *loadgenVUSpecDef) *LoadgenVUSpec {
	if def == nil {
		return nil
	}
	out := &LoadgenVUSpec{
		Identity: LoadgenVUIdentity{
			PoolEndpoint: def.Identity.PoolEndpoint,
			PoolField:    def.Identity.PoolField,
		},
		Actions: make(map[string]LoadgenVUAction, len(def.Actions)),
	}
	for k, a := range def.Actions {
		out.Actions[k] = LoadgenVUAction{
			Method:  a.Method,
			Path:    a.Path,
			Body:    a.Body,
			IDField: a.IDField,
		}
	}
	return out
}

// GetLoadgenIdentity returns the pressure-lane identity param for templateID.
func GetLoadgenIdentity(templateID string) (*LoadgenIdentity, bool) {
	t := GetTemplate(templateID)
	if t == nil || t.Loadgen == nil || t.Loadgen.Identity == nil || t.Loadgen.Identity.Param == "" {
		return nil, false
	}
	cp := *t.Loadgen.Identity
	return &cp, true
}

// validateLoadgenIdentity ensures identity.param names a real endpoint param.
func validateLoadgenIdentity(spec *LoadgenSpec) error {
	if spec == nil || spec.Identity == nil || spec.Identity.Param == "" {
		return nil
	}
	param := spec.Identity.Param
	for _, ep := range spec.Endpoints {
		if ep.ParamName == param {
			return nil
		}
	}
	return fmt.Errorf("loadgen.identity.param %q is not used by any endpoint paramName", param)
}

// LoadgenVU returns a deep copy of the template's VU lane config.
func LoadgenVU(templateID string) (*LoadgenVUSpec, bool) {
	t := GetTemplate(templateID)
	if t == nil || t.Loadgen == nil || t.Loadgen.VU == nil {
		return nil, false
	}
	return loadgenVUSpecFromDef(&loadgenVUSpecDef{
		Identity: loadgenVUIdentityDef{
			PoolEndpoint: t.Loadgen.VU.Identity.PoolEndpoint,
			PoolField:    t.Loadgen.VU.Identity.PoolField,
		},
		Actions: func() map[string]loadgenVUActionDef {
			m := make(map[string]loadgenVUActionDef, len(t.Loadgen.VU.Actions))
			for k, a := range t.Loadgen.VU.Actions {
				m[k] = loadgenVUActionDef{
					Method:  a.Method,
					Path:    a.Path,
					Body:    a.Body,
					IDField: a.IDField,
				}
			}
			return m
		}(),
	}), true
}
