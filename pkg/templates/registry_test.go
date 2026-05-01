package templates

import (
	"encoding/json"
	"strings"
	"testing"
)

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
