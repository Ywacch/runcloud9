package loadgen

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	vegeta "github.com/tsenart/vegeta/v12/lib"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
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

	targeter := buildTargeterFromResolved(targets, 0)
	var vt vegeta.Target
	if err := targeter(&vt); err != nil {
		t.Fatalf("targeter: %v", err)
	}
	if vt.URL == "" || vt.Method == "" {
		t.Fatalf("unexpected target: %+v", vt)
	}
	if vt.Header.Get("X-Forwarded-For") == "" {
		t.Fatal("expected X-Forwarded-For when activeUsers is 0")
	}
}

func TestWeightedTargeter_activeUsersLimitsClientIDs(t *testing.T) {
	t.Parallel()
	targets := []ResolvedTarget{{
		URL:    "http://example.test/r/abc",
		Method: http.MethodGet,
		Weight: 1,
		Key:    "GET /r/{code}",
	}}
	targeter := buildTargeterFromResolved(targets, 3)
	seen := map[string]struct{}{}
	var vt vegeta.Target
	for i := 0; i < 40; i++ {
		if err := targeter(&vt); err != nil {
			t.Fatal(err)
		}
		id := vt.Header.Get(trace.HeaderClientID)
		if id == "" {
			t.Fatal("missing client id")
		}
		seen[id] = struct{}{}
	}
	if len(seen) != 3 {
		t.Fatalf("distinct client ids: got %d want 3 (%v)", len(seen), seen)
	}
}
