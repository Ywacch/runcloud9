package templates

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestIntrospectionConfig(t *testing.T) {
	t.Parallel()
	tmpl := GetTemplate("url-shortener")
	if tmpl == nil {
		t.Fatal("template not found")
	}
	dbSpec, ok := tmpl.Introspection["db"]
	if !ok {
		t.Fatal("db introspection missing")
	}
	if dbSpec.Engine != "postgres" {
		t.Fatalf("want postgres, got %s", dbSpec.Engine)
	}
	if len(dbSpec.Tables) == 0 {
		t.Fatal("no whitelisted tables")
	}
	cacheSpec, ok := tmpl.Introspection["cache"]
	if !ok {
		t.Fatal("cache introspection missing")
	}
	if cacheSpec.Engine != "redis" {
		t.Fatalf("want redis, got %s", cacheSpec.Engine)
	}
}

func TestGetTemplate(t *testing.T) {
	t.Parallel()
	if GetTemplate("unknown") != nil {
		t.Fatal("expected nil for unknown id")
	}
	u := GetTemplate("url-shortener")
	if u == nil || !u.Available || u.Name != "URL Shortener" {
		t.Fatalf("url-shortener: %+v", u)
	}
	if u.Preset == nil || len(u.Preset.Nodes) == 0 || len(u.Preset.Edges) == 0 {
		t.Fatalf("url-shortener preset missing/empty: %+v", u.Preset)
	}
	if u.PresetLite == nil || len(u.PresetLite.Nodes) != 3 {
		t.Fatalf("url-shortener presetLite: %+v", u.PresetLite)
	}
	for _, n := range u.PresetLite.Nodes {
		if n.Type == "load_balancer" || n.Type == "message_queue" || n.Type == "worker" {
			t.Fatalf("presetLite must not include lb/mq/worker: %+v", n)
		}
	}
	for _, n := range u.Preset.Nodes {
		if n.Type == "load_balancer" {
			t.Fatalf("url-shortener preset must not include load balancer: %+v", n)
		}
	}
	if u.Components != 3 {
		t.Fatalf("url-shortener components: want 3, got %d", u.Components)
	}
	if u.SeedConfig == nil || u.SeedConfig.DumpPath != "url-shortener/urls.dump" {
		t.Fatalf("url-shortener seed config: %+v", u.SeedConfig)
	}
	if !u.Simulatable || !u.Deployable {
		t.Fatalf("url-shortener simulatable/deployable: %+v", u)
	}
	e := GetTemplate("ecommerce")
	if e == nil || !e.Available {
		t.Fatalf("ecommerce should exist and be available: %+v", e)
	}
	if !e.Simulatable || !e.Deployable {
		t.Fatalf("ecommerce should be simulatable and deployable: %+v", e)
	}
	if e.Preset == nil {
		t.Fatal("ecommerce preset should be present")
	}
	if e.Components != 5 {
		t.Fatalf("ecommerce components: want 5, got %d", e.Components)
	}
	for _, n := range e.Preset.Nodes {
		if n.Type == "load_balancer" {
			t.Fatalf("ecommerce preset must not include load balancer: %+v", n)
		}
	}
	sf := GetTemplate("social-feed")
	if sf == nil || sf.Preset == nil {
		t.Fatalf("social-feed preset missing: %+v", sf)
	}
	if sf.Components != 5 {
		t.Fatalf("social-feed components: want 5, got %d", sf.Components)
	}
	for _, n := range sf.Preset.Nodes {
		if n.Type == "load_balancer" {
			t.Fatalf("social-feed preset must not include load balancer: %+v", n)
		}
	}
	// Returned pointer must not alias the next call.
	u2 := GetTemplate("url-shortener")
	if u == u2 {
		t.Fatal("GetTemplate should return distinct pointers")
	}
}

func TestIsDeployable(t *testing.T) {
	t.Parallel()
	if !IsDeployable("url-shortener") {
		t.Fatal("url-shortener should be deployable")
	}
	if !IsDeployable("ecommerce") || IsDeployable("nope") {
		t.Fatal("ecommerce should be deployable; unknown should not be deployable")
	}
}

func TestRegistryLen(t *testing.T) {
	t.Parallel()
	if len(Registry()) != 4 {
		t.Fatalf("want 4 templates, got %d", len(Registry()))
	}
}

func TestGetAppServerImage(t *testing.T) {
	t.Parallel()
	img := GetAppServerImage("url-shortener")
	if img == "" || !strings.HasSuffix(img, "/url-shortener:v1") {
		t.Fatalf("GetAppServerImage(url-shortener) = %q", img)
	}
	if GetAppServerImage("unknown") != "" {
		t.Fatal("unknown template should return empty app image")
	}
}

func TestGetWorkerImage(t *testing.T) {
	t.Parallel()
	w := GetWorkerImage("social-feed")
	if w == "" || !strings.Contains(w, "fanout-worker:v1") {
		t.Fatalf("GetWorkerImage(social-feed) = %q", w)
	}
	if GetWorkerImage("url-shortener") != "" {
		t.Fatal("url-shortener should have no worker image")
	}
	if GetWorkerImage("nope") != "" {
		t.Fatal("unknown should return empty worker image")
	}
}

func TestTemplateSpec_SupportsWorker(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"url-shortener":    false,
		"ecommerce":        true,
		"social-feed":      true,
		"social-feed-pull": false,
	}
	for id, want := range cases {
		tmpl := GetTemplate(id)
		if tmpl == nil {
			t.Fatalf("template %q not found", id)
		}
		if tmpl.SupportsWorker != want {
			t.Fatalf("SupportsWorker(%q) = %v want %v", id, tmpl.SupportsWorker, want)
		}
	}
}

func TestImageFor_RegistryEnv(t *testing.T) {
	// Not parallel: mutates process env; parallel tests would race on TEMPLATE_IMAGE_REGISTRY.
	t.Setenv("TEMPLATE_IMAGE_REGISTRY", "reg.example/test")
	got := imageFor("ecommerce")
	wantPrefix := "reg.example/test/ecommerce:v1"
	if got != wantPrefix {
		t.Fatalf("imageFor with env: got %q want %q", got, wantPrefix)
	}
}

func TestImageFor_TagEnv(t *testing.T) {
	// Not parallel: mutates process env; parallel tests would race on TEMPLATE_IMAGE_TAG.
	t.Setenv("TEMPLATE_IMAGE_REGISTRY", "reg.example/test")
	t.Setenv("TEMPLATE_IMAGE_TAG", "git-abc1234")
	got := imageFor("url-shortener")
	want := "reg.example/test/url-shortener:git-abc1234"
	if got != want {
		t.Fatalf("imageFor with TEMPLATE_IMAGE_TAG: got %q want %q", got, want)
	}
}

func TestImageFor_ManifestOverridesEnvTag(t *testing.T) {
	t.Setenv("TEMPLATE_IMAGE_REGISTRY", "reg.example/test")
	t.Setenv("TEMPLATE_IMAGE_TAG", "git-env")
	dir := t.TempDir()
	manifest := dir + "/image-versions.json"
	data := []byte(`{"url-shortener":"git-manifest"}`)
	if err := os.WriteFile(manifest, data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := LoadVersionManifest(manifest); err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	got := imageFor("url-shortener")
	want := "reg.example/test/url-shortener:git-manifest"
	if got != want {
		t.Fatalf("imageFor with manifest override: got %q want %q", got, want)
	}
}

func TestRegistryJSON_IncludesStorytelling(t *testing.T) {
	t.Parallel()
	b, err := json.Marshal(Registry())
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.Contains(s, `"storytelling"`) {
		t.Fatalf("JSON should include storytelling for templates that define it: %s", s)
	}
	us := GetTemplate("url-shortener")
	if us == nil || us.Storytelling == nil || len(us.Storytelling.Roles) == 0 {
		t.Fatalf("url-shortener storytelling missing: %+v", us)
	}
	if _, ok := us.Storytelling.Roles["cache"]; !ok {
		t.Fatal("expected cache role")
	}
	if len(us.Storytelling.Journeys) < 1 {
		t.Fatal("expected journeys")
	}
}

func TestRegistryJSON_IncludesStorytellingEdges(t *testing.T) {
	t.Parallel()
	sf := GetTemplate("social-feed")
	if sf == nil || sf.Storytelling == nil {
		t.Fatal("social-feed storytelling missing")
	}
	edge, ok := sf.Storytelling.Edges["app-server->cache"]
	if !ok || edge.Base == "" {
		t.Fatalf("expected app-server->cache edge meaning, got %+v", sf.Storytelling.Edges)
	}
	if !strings.Contains(strings.ToLower(edge.Base), "timeline") {
		t.Fatalf("social-feed cache edge should mention timelines, got %q", edge.Base)
	}
	us := GetTemplate("url-shortener")
	if us == nil || us.Storytelling == nil || us.Storytelling.Edges["app-server->cache"].Base == "" {
		t.Fatal("url-shortener app-server->cache edge missing")
	}
}

func TestRegistryJSON_IncludesSimulatableDeployable(t *testing.T) {
	t.Parallel()
	b, err := json.Marshal(Registry())
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.Contains(s, `"simulatable"`) || !strings.Contains(s, `"deployable"`) {
		t.Fatalf("JSON missing simulatable/deployable: %s", s)
	}
	if strings.Contains(s, `"appServerImage"`) {
		t.Fatal("appServerImage should be json:\"-\" omitted from API")
	}
	if !strings.Contains(s, `"preset"`) {
		t.Fatal("preset should be included in API JSON")
	}
	if !strings.Contains(s, `"presetLite"`) {
		t.Fatal("presetLite should be included in API JSON")
	}
	if !strings.Contains(s, `"variations"`) {
		t.Fatal("variations should be included in API JSON")
	}
	if strings.Contains(s, `"proxyQueries"`) || strings.Contains(s, "proxy_queries") {
		t.Fatal("ProxyQueries must be json:\"-\" omitted from Registry JSON")
	}
}

func TestTemplateVariations(t *testing.T) {
	t.Parallel()

	cases := []struct {
		id      string
		wantLen int
		wantIDs []string
	}{
		{
			id:      "url-shortener",
			wantLen: 3,
			wantIDs: []string{"app-db", "app-db-cache", "app-db-cache-proxy"},
		},
		{
			id:      "ecommerce",
			wantLen: 3,
			wantIDs: []string{"app-db", "app-db-cache", "app-db-cache-mq-worker"},
		},
		{
			id:      "social-feed",
			wantLen: 2,
			wantIDs: []string{"timeline-sync", "timeline-async"},
		},
		{
			id:      "social-feed-pull",
			wantLen: 1,
			wantIDs: []string{"app-db"},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.id, func(t *testing.T) {
			t.Parallel()
			tmpl := GetTemplate(tc.id)
			if tmpl == nil {
				t.Fatal("template not found")
			}
			if len(tmpl.Variations) != tc.wantLen {
				t.Fatalf("variations len: got %d want %d", len(tmpl.Variations), tc.wantLen)
			}
			for i, wantID := range tc.wantIDs {
				got := tmpl.Variations[i]
				if got.ID != wantID {
					t.Fatalf("variation[%d] id: got %q want %q", i, got.ID, wantID)
				}
				if got.Label == "" || got.Description == "" {
					t.Fatalf("variation %q missing label/description: %+v", got.ID, got)
				}
				if len(got.Nodes) == 0 || len(got.Edges) == 0 {
					t.Fatalf("variation %q missing nodes/edges", got.ID)
				}
			}
		})
	}

	us := GetTemplate("url-shortener")
	if us == nil {
		t.Fatal("url-shortener missing")
	}
	var proxy *TemplateVariation
	for i := range us.Variations {
		if us.Variations[i].ID == "app-db-cache-proxy" {
			proxy = &us.Variations[i]
			break
		}
	}
	if proxy == nil {
		t.Fatal("app-db-cache-proxy variation missing")
	}
	hasAppDB := false
	for _, e := range proxy.Edges {
		if e.SourceID == "app-1" && e.TargetID == "db-1" {
			hasAppDB = true
		}
	}
	if hasAppDB {
		t.Fatal("proxy variation must not include app->db edge")
	}
	for _, n := range proxy.Nodes {
		if n.ID == "cache-1" {
			if got := n.Config["strategy"]; got != "write-through" {
				t.Fatalf("proxy cache strategy: got %v want write-through", got)
			}
		}
	}
}

func TestLiteDisplayMetadata(t *testing.T) {
	t.Parallel()

	cases := []struct {
		id                 string
		wantComponentsLite int
		wantBottleneckLite string
	}{
		{
			id:                 "url-shortener",
			wantComponentsLite: 3,
			wantBottleneckLite: "Cache miss → DB query amplification",
		},
		{
			id:                 "ecommerce",
			wantComponentsLite: 3,
			wantBottleneckLite: "Write contention on shared database",
		},
		{
			id:                 "social-feed",
			wantComponentsLite: 3,
			wantBottleneckLite: "Sync fan-out on POST scales with follower count",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.id, func(t *testing.T) {
			t.Parallel()
			tmpl := GetTemplate(tc.id)
			if tmpl == nil {
				t.Fatal("template not found")
			}
			if tmpl.ComponentsLite != tc.wantComponentsLite {
				t.Fatalf("componentsLite: got %d want %d", tmpl.ComponentsLite, tc.wantComponentsLite)
			}
			if tmpl.BottleneckLite != tc.wantBottleneckLite {
				t.Fatalf("bottleneckLite: got %q want %q", tmpl.BottleneckLite, tc.wantBottleneckLite)
			}
			if tmpl.DescriptionLite == "" {
				t.Fatal("descriptionLite should be set")
			}
			if strings.Contains(strings.ToLower(tmpl.DescriptionLite), "load balancer") {
				t.Fatalf("descriptionLite should not mention load balancer: %q", tmpl.DescriptionLite)
			}
		})
	}

	sfp := GetTemplate("social-feed-pull")
	if sfp == nil {
		t.Fatal("social-feed-pull missing")
	}
	if sfp.DescriptionLite != "" || sfp.ComponentsLite != 0 || sfp.BottleneckLite != "" {
		t.Fatalf("social-feed-pull should not define lite metadata: %+v", sfp)
	}
}

func TestRegistryJSON_IncludesLiteDisplayMetadata(t *testing.T) {
	t.Parallel()
	b, err := json.Marshal(GetTemplate("social-feed"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, key := range []string{`"descriptionLite"`, `"componentsLite"`, `"bottleneckLite"`} {
		if !strings.Contains(s, key) {
			t.Fatalf("JSON missing %s: %s", key, s)
		}
	}
}

func TestSocialFeedPostCreate_SyncPushExplanationVariant(t *testing.T) {
	t.Parallel()
	sf := GetTemplate("social-feed")
	if sf == nil || sf.Storytelling == nil {
		t.Fatal("social-feed storytelling missing")
	}
	var postCreate *JourneySpec
	for i := range sf.Storytelling.Journeys {
		if sf.Storytelling.Journeys[i].ID == "post-create" {
			postCreate = &sf.Storytelling.Journeys[i]
			break
		}
	}
	if postCreate == nil {
		t.Fatal("post-create journey missing")
	}
	found := false
	for _, v := range postCreate.ExplanationVariants {
		if v.When.Topology == "hasCache+!hasMQ" {
			found = true
			if !strings.Contains(v.Text, "Sync push") {
				t.Fatalf("variant text unexpected: %q", v.Text)
			}
			break
		}
	}
	if !found {
		t.Fatal("expected hasCache+!hasMQ explanation variant on post-create")
	}
}

func TestEmbeddedProxyQueries(t *testing.T) {
	t.Parallel()
	us := GetTemplate("url-shortener")
	if us == nil || len(us.ProxyQueries) != 1 {
		t.Fatalf("url-shortener: want 1 proxyQueries, got %v", us)
	}
	if us.ProxyQueries[0].Pattern != "^([^:]+)$" || !strings.Contains(us.ProxyQueries[0].SQL, "original") {
		t.Fatalf("url-shortener proxy rule unexpected: %+v", us.ProxyQueries[0])
	}

	ec := GetTemplate("ecommerce")
	if ec == nil || len(ec.ProxyQueries) != 3 {
		t.Fatalf("ecommerce: want 3 proxyQueries, got %d %+v", len(ec.ProxyQueries), ec)
	}
	found := map[string]bool{}
	for _, r := range ec.ProxyQueries {
		found[r.Pattern] = true
		if r.Pattern == `^catalog$` && !strings.Contains(strings.ToLower(r.SQL), "products") {
			t.Fatalf("catalog sql missing products: %q", r.SQL)
		}
	}
	for _, p := range []string{`^catalog$`, `^product:(.+)$`, `^product-pool$`} {
		if !found[p] {
			t.Fatalf("ecommerce missing pattern %q in %+v", p, ec.ProxyQueries)
		}
	}

	sf := GetTemplate("social-feed")
	if sf == nil {
		t.Fatal("social-feed missing")
	}
	if len(sf.ProxyQueries) != 0 {
		t.Fatalf("social-feed proxyQueries deferred — want empty, got %+v", sf.ProxyQueries)
	}
}

func TestLoadgenDefaultRPSByTemplate(t *testing.T) {
	t.Parallel()

	sfp := GetTemplate("social-feed-pull")
	if sfp == nil || sfp.LoadgenDefaultRPS != 25 {
		t.Fatalf("social-feed-pull loadgenDefaultRps: got %+v", sfp)
	}
	us := GetTemplate("url-shortener")
	if us == nil || us.LoadgenDefaultRPS != 0 {
		t.Fatalf("url-shortener loadgenDefaultRps: want 0 (unset), got %+v", us)
	}
}

func TestLoadgenEndpointsFromYAML_matchLegacyGoMap(t *testing.T) {
	t.Parallel()

	legacy := map[string][]LoadgenEndpointSpec{
		"url-shortener": {
			{Method: "GET", PathTemplate: "/r/{code}", ParamName: "code", PoolEndpoint: "/code-pool", PoolField: "codes", Weight: 80},
			{Method: "POST", PathTemplate: "/shorten", BodyTemplate: `{"url":"https://example.com/page/{{.Rand}}"}`, Weight: 20},
		},
		"ecommerce": {
			{Method: "GET", PathTemplate: "/catalog", Weight: 40},
			{Method: "GET", PathTemplate: "/products/{productId}", ParamName: "productId", PoolEndpoint: "/product-pool", PoolField: "productIds", Weight: 30},
			{Method: "POST", PathTemplate: "/checkout", ParamName: "productId", PoolEndpoint: "/product-pool", PoolField: "productIds", BodyTemplate: `{"product_id":{{.Param}}, "quantity":1}`, Weight: 30},
		},
		"social-feed": {
			{Method: "GET", PathTemplate: "/feed/{userId}", ParamName: "userId", PoolEndpoint: "/user-pool", PoolField: "userIds", Weight: 60},
			{Method: "POST", PathTemplate: "/post", ParamName: "userId", PoolEndpoint: "/user-pool", PoolField: "userIds", BodyTemplate: `{"user_id":{{.Param}}, "content":"Load test post {{.Rand}}"}`, Weight: 30},
			{Method: "GET", PathTemplate: "/users/{userId}", ParamName: "userId", PoolEndpoint: "/user-pool", PoolField: "userIds", Weight: 10},
		},
		"social-feed-pull": {
			{Method: "GET", PathTemplate: "/feed/{userId}", ParamName: "userId", PoolEndpoint: "/user-pool", PoolField: "userIds", Weight: 60},
			{Method: "POST", PathTemplate: "/post", ParamName: "userId", PoolEndpoint: "/user-pool", PoolField: "userIds", BodyTemplate: `{"user_id":{{.Param}}, "content":"Load test post {{.Rand}}"}`, Weight: 30},
			{Method: "GET", PathTemplate: "/users/{userId}", ParamName: "userId", PoolEndpoint: "/user-pool", PoolField: "userIds", Weight: 10},
		},
	}

	for templateID, want := range legacy {
		got, ok := LoadgenEndpoints(templateID)
		if !ok {
			t.Fatalf("%s: LoadgenEndpoints returned false", templateID)
		}
		if len(got) != len(want) {
			t.Fatalf("%s: len(got)=%d len(want)=%d", templateID, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s[%d]: got %+v want %+v", templateID, i, got[i], want[i])
			}
		}
	}
}

func TestLoadgenSchedulesFromYAML(t *testing.T) {
	t.Parallel()

	sched, ok := GetLoadgenSchedule("social-feed", "world_cup_goal")
	if !ok {
		t.Fatal("world_cup_goal missing")
	}
	if len(sched.Phases) != 4 {
		t.Fatalf("phases len: got %d", len(sched.Phases))
	}
	if sched.Phases[1].ActorOverride["celebrityPostPct"] != 90 {
		t.Fatalf("spike override: %+v", sched.Phases[1])
	}
}

func TestLoadgenActorParamsFromYAML_ecommerce(t *testing.T) {
	t.Parallel()

	params, ok := LoadgenParams("ecommerce")
	if !ok || len(params) != 1 {
		t.Fatalf("ecommerce params: ok=%v len=%d", ok, len(params))
	}
	if params[0].Key != "readAfterPostBoost" || params[0].Default != 2 {
		t.Fatalf("readAfterPostBoost: %+v", params[0])
	}
	if params[0].Min == nil || *params[0].Min != 1 || params[0].Max == nil || *params[0].Max != 5 {
		t.Fatalf("readAfterPostBoost bounds: %+v", params[0])
	}
}

func TestLoadgenActorParamsFromYAML(t *testing.T) {
	t.Parallel()

	params, ok := LoadgenParams("social-feed")
	if !ok || len(params) != 2 {
		t.Fatalf("social-feed params: ok=%v len=%d", ok, len(params))
	}
	if params[0].Key != "celebrityPostPct" || params[0].Default != 5 {
		t.Fatalf("celebrityPostPct: %+v", params[0])
	}
	if len(params[0].Pools) != 2 || params[0].Pools[0].Name != "celebrity" {
		t.Fatalf("pools: %+v", params[0].Pools)
	}
	if params[1].Key != "readAfterPostBoost" || params[1].Min == nil || *params[1].Min != 1 {
		t.Fatalf("readAfterPostBoost: %+v", params[1])
	}
}

func TestLoadgenConfig_socialFeed(t *testing.T) {
	t.Parallel()
	view, ok := LoadgenConfig("social-feed")
	if !ok {
		t.Fatal("expected social-feed loadgen config")
	}
	if view.Identity == nil || view.Identity.Param != "userId" {
		t.Fatalf("identity: %+v", view.Identity)
	}
	ids := map[string]bool{}
	for _, s := range view.Schedules {
		ids[s.ID] = true
	}
	if !ids["steady"] || !ids["world_cup_goal"] {
		t.Fatalf("schedules: %+v", view.Schedules)
	}
	found := false
	for _, p := range view.Params {
		if p.Key == "celebrityPostPct" {
			found = true
		}
	}
	if !found {
		t.Fatalf("params: %+v", view.Params)
	}
}

func TestLoadgenConfig_unknown(t *testing.T) {
	t.Parallel()
	if _, ok := LoadgenConfig("no-such-template"); ok {
		t.Fatal("expected missing template")
	}
}

func TestSocialFeedPull_NoRedisInTopologyOrPreset(t *testing.T) {
	t.Parallel()
	sfp := GetTemplate("social-feed-pull")
	if sfp == nil {
		t.Fatal("social-feed-pull missing")
	}
	if sfp.Components != 3 {
		t.Fatalf("social-feed-pull components: want 3, got %d", sfp.Components)
	}
	for _, th := range sfp.Topology {
		if th.Type == "cache" {
			t.Fatalf("social-feed-pull topology must not include cache, got %+v", sfp.Topology)
		}
	}
	if sfp.Preset == nil {
		t.Fatal("social-feed-pull preset missing")
	}
	for _, n := range sfp.Preset.Nodes {
		if n.Type == "cache" {
			t.Fatalf("social-feed-pull preset must not include cache node, got %+v", n)
		}
		if n.Type == "load_balancer" {
			t.Fatalf("social-feed-pull preset must not include load balancer, got %+v", n)
		}
	}
	for _, e := range sfp.Preset.Edges {
		if e.TargetID == "cache-1" || e.SourceID == "cache-1" || strings.Contains(e.ID, "cache") {
			t.Fatalf("social-feed-pull preset must not include cache edges, got %+v", e)
		}
	}
}

func TestAppDatabaseModeByTemplate(t *testing.T) {
	t.Parallel()

	us := GetTemplate("url-shortener")
	if us == nil || us.AppDatabaseMode != "non-proxy" {
		t.Fatalf("url-shortener appDatabaseMode: got %+v", us)
	}

	ec := GetTemplate("ecommerce")
	if ec == nil || ec.AppDatabaseMode != "always" {
		t.Fatalf("ecommerce appDatabaseMode: got %+v", ec)
	}

	sf := GetTemplate("social-feed")
	if sf == nil || sf.AppDatabaseMode != "always" {
		t.Fatalf("social-feed appDatabaseMode: got %+v", sf)
	}

	sfp := GetTemplate("social-feed-pull")
	if sfp == nil || sfp.AppDatabaseMode != "always" {
		t.Fatalf("social-feed-pull appDatabaseMode: got %+v", sfp)
	}
}

func TestWorkerTimelineStoreByTemplate(t *testing.T) {
	t.Parallel()

	sf := GetTemplate("social-feed")
	if sf == nil || sf.WorkerTimelineStore != "redis" {
		t.Fatalf("social-feed workerTimelineStore: got %+v", sf)
	}

	us := GetTemplate("url-shortener")
	if us == nil || us.WorkerTimelineStore != "none" {
		t.Fatalf("url-shortener workerTimelineStore: got %+v", us)
	}
}

func TestSocialFeedPreset_UsesCanonicalWorkerToMQEdge(t *testing.T) {
	t.Parallel()
	sf := GetTemplate("social-feed")
	if sf == nil || sf.Preset == nil {
		t.Fatalf("social-feed preset missing: %+v", sf)
	}
	hasWorkerToMQ := false
	hasMQToWorker := false
	for _, e := range sf.Preset.Edges {
		if e.SourceID == "worker-1" && e.TargetID == "mq-1" {
			hasWorkerToMQ = true
		}
		if e.SourceID == "mq-1" && e.TargetID == "worker-1" {
			hasMQToWorker = true
		}
	}
	if !hasWorkerToMQ {
		t.Fatal("social-feed preset missing canonical worker->mq edge")
	}
	if hasMQToWorker {
		t.Fatal("social-feed preset must not include legacy mq->worker edge")
	}
}

func TestSocialFeedPreset_LabelsRedisAsTimelineStore(t *testing.T) {
	t.Parallel()

	sf := GetTemplate("social-feed")
	if sf == nil || sf.Preset == nil {
		t.Fatalf("social-feed preset missing: %+v", sf)
	}
	for _, n := range sf.Preset.Nodes {
		if n.ID != "cache-1" {
			continue
		}
		if got := n.Config["strategy"]; got != "timeline-store" {
			t.Fatalf("social-feed cache strategy: want timeline-store, got %v", got)
		}
		if got := n.Config["cacheStrategy"]; got != "timeline-store" {
			t.Fatalf("social-feed cacheStrategy: want timeline-store, got %v", got)
		}
		return
	}
	t.Fatal("social-feed preset missing cache-1")
}
