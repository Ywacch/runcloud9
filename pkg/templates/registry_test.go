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
	if len(Registry()) != 3 {
		t.Fatalf("want 3 templates, got %d", len(Registry()))
	}
}

func TestGetAppServerImage(t *testing.T) {
	t.Parallel()
	img := GetAppServerImage("url-shortener")
	if img == "" || !strings.HasSuffix(img, "/url-shortener:v5") {
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
}
