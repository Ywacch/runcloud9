package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/rules"
)

func TestHandleMetaJobs_ETag(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/meta/jobs", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header")
	}

	var resp engines.Catalog
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Version != engines.CatalogVersion {
		t.Fatalf("catalogVersion: want %q, got %q", engines.CatalogVersion, resp.Version)
	}
	if len(resp.Jobs) != 4 || len(resp.Engines) != 3 || len(resp.Pairs) != 6 {
		t.Fatalf("counts: jobs=%d engines=%d pairs=%d", len(resp.Jobs), len(resp.Engines), len(resp.Pairs))
	}
	for _, e := range resp.Engines {
		if e.MinShape.Pods < 1 {
			t.Fatalf("engine %s missing minShape", e.ID)
		}
	}
	for _, p := range resp.Pairs {
		if p.Fidelity == "" {
			t.Fatalf("pair %s/%s missing fidelity", p.Job, p.Engine)
		}
	}
	for _, card := range resp.Guarantees {
		for _, e := range card.Entries {
			if e.Fidelity == "" {
				t.Fatalf("guarantee %q missing fidelity", e.ID)
			}
		}
	}

	req304 := httptest.NewRequest(http.MethodGet, "/meta/jobs", nil)
	req304.Header.Set("If-None-Match", etag)
	rec304 := httptest.NewRecorder()
	mux.ServeHTTP(rec304, req304)

	if rec304.Code != http.StatusNotModified {
		t.Fatalf("conditional status: want 304, got %d body=%s", rec304.Code, rec304.Body.String())
	}
	if rec304.Body.Len() != 0 {
		t.Fatalf("304 body should be empty, got %q", rec304.Body.String())
	}
}

func TestEnginesFidelityMatchesRules(t *testing.T) {
	t.Parallel()
	if engines.FidelityReal != rules.FidelityReal {
		t.Fatalf("real: engines=%q rules=%q", engines.FidelityReal, rules.FidelityReal)
	}
	if engines.FidelitySimulated != rules.FidelitySimulated {
		t.Fatalf("simulated: engines=%q rules=%q", engines.FidelitySimulated, rules.FidelitySimulated)
	}
	if engines.FidelityCompressed != rules.FidelityCompressed {
		t.Fatalf("compressed: engines=%q rules=%q", engines.FidelityCompressed, rules.FidelityCompressed)
	}
}
