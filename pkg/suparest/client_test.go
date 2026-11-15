package suparest

import "testing"

func TestHeadersOpaqueKeyOmitsBearer(t *testing.T) {
	h := headers("sb_secret_example")
	if _, ok := h["Authorization"]; ok {
		t.Fatal("sb_secret key must not set Authorization")
	}
	if h["apikey"] != "sb_secret_example" {
		t.Fatalf("apikey = %q", h["apikey"])
	}
}

func TestHeadersJWTSetsBearer(t *testing.T) {
	h := headers("eyJexample")
	if h["Authorization"] != "Bearer eyJexample" {
		t.Fatalf("Authorization = %q", h["Authorization"])
	}
	if h["apikey"] != "eyJexample" {
		t.Fatalf("apikey = %q", h["apikey"])
	}
}
