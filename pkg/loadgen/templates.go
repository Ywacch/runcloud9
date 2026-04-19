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
	"ecommerce": {
		{
			Method:       "GET",
			PathTemplate: "/catalog",
			Weight:       40,
		},
		{
			Method:       "GET",
			PathTemplate: "/products/{productId}",
			ParamName:    "productId",
			PoolEndpoint: "/product-pool",
			PoolField:    "productIds",
			Weight:       30,
		},
		{
			Method:       "POST",
			PathTemplate: "/checkout",
			BodyTemplate: `{"product_id":{{.Param}}, "quantity":1}`,
			ParamName:    "productId",
			PoolEndpoint: "/product-pool",
			PoolField:    "productIds",
			Weight:       30,
		},
	},
	"social-feed": {
		{
			Method:       "GET",
			PathTemplate: "/feed/{userId}",
			ParamName:    "userId",
			PoolEndpoint: "/user-pool",
			PoolField:    "userIds",
			Weight:       60,
		},
		{
			Method:       "POST",
			PathTemplate: "/post",
			BodyTemplate: `{"user_id":{{.Param}}, "content":"Load test post {{.Rand}}"}`,
			ParamName:    "userId",
			PoolEndpoint: "/user-pool",
			PoolField:    "userIds",
			Weight:       30,
		},
		{
			Method:       "GET",
			PathTemplate: "/users/{userId}",
			ParamName:    "userId",
			PoolEndpoint: "/user-pool",
			PoolField:    "userIds",
			Weight:       10,
		},
	},
	"social-feed-pull": {
		{
			Method:       "GET",
			PathTemplate: "/feed/{userId}",
			ParamName:    "userId",
			PoolEndpoint: "/user-pool",
			PoolField:    "userIds",
			Weight:       60,
		},
		{
			Method:       "POST",
			PathTemplate: "/post",
			BodyTemplate: `{"user_id":{{.Param}}, "content":"Load test post {{.Rand}}"}`,
			ParamName:    "userId",
			PoolEndpoint: "/user-pool",
			PoolField:    "userIds",
			Weight:       30,
		},
		{
			Method:       "GET",
			PathTemplate: "/users/{userId}",
			ParamName:    "userId",
			PoolEndpoint: "/user-pool",
			PoolField:    "userIds",
			Weight:       10,
		},
	},
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
