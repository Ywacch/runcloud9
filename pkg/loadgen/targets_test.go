package loadgen

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

func TestProfileToTargets_StaticAndPoolExpansion(t *testing.T) {
	// Do not run parallel: t.Setenv is incompatible with t.Parallel(), and a developer
	// LOADGEN_BASE_URL (e.g. local port-forward) must not override the httptest server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/code-pool":
			_ = json.NewEncoder(w).Encode(map[string]any{"codes": []string{"abc", "def"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	t.Setenv("LOADGEN_BASE_URL", srv.URL)

	profile := NormalizeProfile(LoadProfile{
		Template: "url-shortener",
		RPS:      50,
		BaseURL:  srv.URL,
	})

	targets, dist, err := profileToTargets(context.Background(), "ns-test", profile)
	if err != nil {
		t.Fatalf("profileToTargets: %v", err)
	}
	if len(dist) != 2 {
		t.Fatalf("distribution keys: got %d want 2", len(dist))
	}

	var redirectCount int
	for _, tgt := range targets {
		if strings.Contains(tgt.URL, "/r/") {
			redirectCount++
		}
	}
	if redirectCount != 2 {
		t.Fatalf("expanded redirect targets: got %d want 2", redirectCount)
	}

	targeter := buildTargeterFromResolved(targets)
	var vt vegeta.Target
	if err := targeter(&vt); err != nil {
		t.Fatalf("targeter: %v", err)
	}
	if vt.URL == "" || vt.Method == "" {
		t.Fatalf("unexpected target: %+v", vt)
	}
}
