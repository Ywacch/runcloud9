package loadgen

import "github.com/ywacch/cloud9/pkg/templates"

// cloneEndpointSpecs returns a deep copy of registry specs for a template (safe to mutate weights).
func cloneEndpointSpecs(template string) ([]EndpointSpec, bool) {
	src, ok := templates.LoadgenEndpoints(template)
	if !ok {
		return nil, false
	}
	out := make([]EndpointSpec, len(src))
	for i, s := range src {
		out[i] = endpointSpecFromTemplate(s)
	}
	return out, true
}

func endpointSpecFromTemplate(s templates.LoadgenEndpointSpec) EndpointSpec {
	return EndpointSpec{
		Method:       s.Method,
		PathTemplate: s.PathTemplate,
		ParamName:    s.ParamName,
		PoolEndpoint: s.PoolEndpoint,
		PoolField:    s.PoolField,
		BodyTemplate: s.BodyTemplate,
		Weight:       s.Weight,
	}
}
