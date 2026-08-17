package loadgen

import (
	"net/http"
	"strings"
	"testing"
)

func TestMergeActorParamValues_rejectsUnknownKey(t *testing.T) {
	t.Parallel()
	_, err := mergeActorParamValues("social-feed", map[string]float64{"noSuchParam": 1})
	if err == nil || !strings.Contains(err.Error(), "unknown actor param") {
		t.Fatalf("expected unknown param error, got %v", err)
	}
}

func TestMergeActorParamValues_rejectsOutOfRange(t *testing.T) {
	t.Parallel()
	_, err := mergeActorParamValues("social-feed", map[string]float64{"readAfterPostBoost": 10})
	if err == nil || !strings.Contains(err.Error(), "above maximum") {
		t.Fatalf("expected max error, got %v", err)
	}
}

func TestMergeActorParamValues_fillsDefaults(t *testing.T) {
	t.Parallel()
	out, err := mergeActorParamValues("social-feed", nil)
	if err != nil {
		t.Fatal(err)
	}
	if out["celebrityPostPct"] != 5 || out["readAfterPostBoost"] != 1 {
		t.Fatalf("defaults: got %+v", out)
	}
}

func TestActorModel_stableIdentity(t *testing.T) {
	t.Parallel()
	specs := []EndpointSpec{
		{Method: http.MethodGet, PathTemplate: "/feed/{userId}", ParamName: "userId", Weight: 50},
		{Method: http.MethodGet, PathTemplate: "/users/{userId}", ParamName: "userId", Weight: 50},
	}
	pools := map[string][]string{"userId": {"42", "99"}}
	m := newActorModel("http://test", specs, nil, pools, nil, nil, map[string]float64{"readAfterPostBoost": 1})

	var firstID string
	for i := 0; i < 20; i++ {
		rt := m.nextResolved()
		if !strings.Contains(rt.URL, "/feed/") && !strings.Contains(rt.URL, "/users/") {
			t.Fatalf("unexpected url: %s", rt.URL)
		}
		id := strings.TrimPrefix(rt.URL, "http://test")
		if strings.HasPrefix(id, "/feed/") {
			id = strings.TrimPrefix(id, "/feed/")
		} else {
			id = strings.TrimPrefix(id, "/users/")
		}
		if firstID == "" {
			firstID = id
			continue
		}
		if id != firstID {
			t.Fatalf("request %d: identity changed %q -> %q", i, firstID, id)
		}
	}
}

func TestActorModel_segmentSkew(t *testing.T) {
	t.Parallel()
	specs := []EndpointSpec{
		{Method: http.MethodPost, PathTemplate: "/post", ParamName: "userId", BodyTemplate: `{"id":{{.Param}}}`, Weight: 100},
	}
	paramDefs := []ActorParam{
		{
			Key:  "skewPct",
			Type: "percent",
			Pools: []ActorPool{
				{Name: "alpha", PoolEndpoint: "/pool?segment=alpha", PoolField: "ids"},
				{Name: "beta", PoolEndpoint: "/pool?segment=beta", PoolField: "ids"},
			},
			ApplyTo: []string{"POST /post"},
		},
	}
	segmentPools := map[string][]string{
		segmentPoolKey("/pool?segment=alpha", "ids", "alpha"): {"1", "2"},
		segmentPoolKey("/pool?segment=beta", "ids", "beta"):   {"9", "8"},
	}
	m := newActorModel(
		"http://test",
		specs,
		nil,
		nil,
		segmentPools,
		paramDefs,
		map[string]float64{"skewPct": 100},
	)

	alphaHits := 0
	for i := 0; i < 100; i++ {
		rt := m.nextResolved()
		if strings.Contains(rt.Body, "1") || strings.Contains(rt.Body, "2") {
			alphaHits++
		}
	}
	if alphaHits < 80 {
		t.Fatalf("expected ~100%% alpha segment hits at skewPct=100, got %d/100", alphaHits)
	}
}

func TestActorModel_readAfterPostBoost(t *testing.T) {
	t.Parallel()
	specs := []EndpointSpec{
		{Method: http.MethodPost, PathTemplate: "/post", ParamName: "userId", BodyTemplate: `{"user_id":{{.Param}}}`, Weight: 1},
		{Method: http.MethodGet, PathTemplate: "/feed/{userId}", ParamName: "userId", Weight: 99},
	}
	pools := map[string][]string{"userId": {"111", "222", "333"}}
	m := newActorModel(
		"http://test",
		specs,
		map[string]int{"POST /post": 1, "GET /feed/{userId}": 99},
		pools,
		nil,
		nil,
		map[string]float64{"readAfterPostBoost": 100},
	)

	var posted string
	for i := 0; i < 100; i++ {
		rt := m.nextResolved()
		if rt.Method == http.MethodPost {
			posted = strings.TrimPrefix(rt.Body, `{"user_id":`)
			posted = strings.TrimSuffix(posted, "}")
			break
		}
	}
	if posted == "" {
		t.Fatal("failed to observe POST within 100 requests")
	}

	matched := 0
	for i := 0; i < 50; i++ {
		rt := m.nextResolved()
		if strings.Contains(rt.URL, "/feed/"+posted) {
			matched++
		}
	}
	if matched < 40 {
		t.Fatalf("expected read-after-post boost to reuse %q, matched %d/50", posted, matched)
	}
}

func TestProfileToTargetPlan_actorModeForTemplatesWithParams(t *testing.T) {
	t.Parallel()
	plan, err := profileToTargetPlan(t.Context(), "ns", NormalizeProfile(LoadProfile{
		Template: "social-feed",
		RPS:      10,
		BaseURL:  "http://example",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Actor == nil {
		t.Fatal("social-feed with params should use actor mode")
	}
	if len(plan.Static) != 0 {
		t.Fatalf("actor mode should not expand static targets, got %d", len(plan.Static))
	}
}

func TestBuildPodStartBody_actorMode(t *testing.T) {
	t.Parallel()
	body, err := BuildPodStartBody(t.Context(), "ns", NormalizeProfile(LoadProfile{
		Template: "social-feed",
		RPS:      10,
		BaseURL:  "http://example",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if body.Actor == nil {
		t.Fatal("expected actor config in pod body")
	}
	if len(body.Targets) != 0 {
		t.Fatalf("actor mode should omit static targets, got %d", len(body.Targets))
	}
	if body.Actor.ParamValues["celebrityPostPct"] != 5 {
		t.Fatalf("param defaults: %+v", body.Actor.ParamValues)
	}
}
