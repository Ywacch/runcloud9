package templates

import "testing"

func TestGetTemplate(t *testing.T) {
	t.Parallel()
	if GetTemplate("unknown") != nil {
		t.Fatal("expected nil for unknown id")
	}
	u := GetTemplate("url-shortener")
	if u == nil || !u.Available || u.Name != "URL Shortener" {
		t.Fatalf("url-shortener: %+v", u)
	}
	e := GetTemplate("ecommerce")
	if e == nil || e.Available {
		t.Fatalf("ecommerce should exist and be unavailable: %+v", e)
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
	if IsDeployable("ecommerce") || IsDeployable("nope") {
		t.Fatal("ecommerce and unknown should not be deployable")
	}
}

func TestRegistryLen(t *testing.T) {
	t.Parallel()
	if len(Registry()) != 3 {
		t.Fatalf("want 3 templates, got %d", len(Registry()))
	}
}
