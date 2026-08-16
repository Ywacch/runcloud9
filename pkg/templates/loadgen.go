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
	DefaultRps int                  `yaml:"defaultRps"`
	Endpoints  []loadgenEndpointDef `yaml:"endpoints"`
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

func loadgenSpecFromDef(def *loadgenDef) *LoadgenSpec {
	if def == nil {
		return nil
	}
	spec := &LoadgenSpec{
		DefaultRPS: def.DefaultRps,
		Endpoints:  make([]LoadgenEndpointSpec, 0, len(def.Endpoints)),
	}
	for _, ep := range def.Endpoints {
		spec.Endpoints = append(spec.Endpoints, LoadgenEndpointSpec(ep))
	}
	return spec
}
