package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ywacch/cloud9/pkg/rules"
)

func TestHandleGetRules_ReturnsCatalogWithETag(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/meta/rules", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type: want application/json, got %q", ct)
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header")
	}

	var resp rulesCatalogResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.CatalogVersion != rules.CatalogVersion {
		t.Fatalf("catalogVersion: want %q, got %q", rules.CatalogVersion, resp.CatalogVersion)
	}
	if len(resp.Rules) != 20 {
		t.Fatalf("rules: want 20, got %d", len(resp.Rules))
	}
	if resp.Rules[0].ID == "" || resp.Rules[0].Family == "" || resp.Rules[0].Summary == "" {
		t.Fatalf("first rule missing fields: %+v", resp.Rules[0])
	}

	req304 := httptest.NewRequest(http.MethodGet, "/meta/rules", nil)
	req304.Header.Set("If-None-Match", etag)
	rec304 := httptest.NewRecorder()
	mux.ServeHTTP(rec304, req304)

	if rec304.Code != http.StatusNotModified {
		t.Fatalf("conditional status: want 304, got %d body=%s", rec304.Code, rec304.Body.String())
	}
	if rec304.Header().Get("ETag") != etag {
		t.Fatalf("304 ETag: want %q, got %q", etag, rec304.Header().Get("ETag"))
	}
	if rec304.Body.Len() != 0 {
		t.Fatalf("304 body should be empty, got %q", rec304.Body.String())
	}
}
