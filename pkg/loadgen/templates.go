package loadgen

// TemplateEndpoints maps template name to its endpoint specs.
// This is the ONLY place that knows about specific templates.
// Adding a new template = adding one entry here. The rest of the loadgen
// package is completely generic.
var TemplateEndpoints = map[string][]EndpointSpec{
	"url-shortener": {
		{
			Method:       "GET",
			PathTemplate: "/r/{code}",
			ParamName:    "code",
			PoolEndpoint: "/code-pool",
			PoolField:    "codes",
			Weight:       80,
		},
		{
			Method:       "POST",
			PathTemplate: "/shorten",
			BodyTemplate: `{"url":"https://example.com/page/{{.Rand}}"}`,
			Weight:       20,
		},
	},
	// Future: "social-feed", "e-commerce", etc. — add pool HTTP handlers on each app image.
}

// cloneEndpointSpecs returns a deep copy of registry specs for a template (safe to mutate weights).
func cloneEndpointSpecs(template string) ([]EndpointSpec, bool) {
	src, ok := TemplateEndpoints[template]
	if !ok || len(src) == 0 {
		return nil, false
	}
	out := make([]EndpointSpec, len(src))
	copy(out, src)
	return out, true
}
