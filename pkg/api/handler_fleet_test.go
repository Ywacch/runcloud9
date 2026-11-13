package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

func TestFleet_NotFoundWhenEnvEmpty(t *testing.T) {
	t.Setenv("FLEET_LIST_USER_ID", "")
	mux := fleetTestMux(newFakeDeploymentStore())
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/fleet", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestFleet_ForbiddenForOtherUser(t *testing.T) {
	t.Setenv("FLEET_LIST_USER_ID", "probe-user")
	mux := fleetTestMux(newFakeDeploymentStore())
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/fleet", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestFleet_ReturnsActiveRows(t *testing.T) {
	t.Setenv("FLEET_LIST_USER_ID", "dev-user")
	fake := newFakeDeploymentStore()
	when := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	fake.fleet = []store.DeploymentRow{{
		ID:        "dep-1",
		Namespace: "dep-1",
		Status:    "running",
		Template:  "url-shortener-small",
		ExpiresAt: when,
		CreatedAt: when,
	}}
	mux := fleetTestMux(fake)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/fleet", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body %s", rec.Code, rec.Body.String())
	}
	var got []fleetItem
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].ID != "dep-1" || got[0].Status != "running" {
		t.Fatalf("body = %+v", got)
	}
	if got[0].ExpiresAt != when.Format(time.RFC3339) {
		t.Fatalf("expiresAt = %s", got[0].ExpiresAt)
	}
}

func fleetTestMux(store HandlerDeploymentStore) *http.ServeMux {
	h := NewHandler(nil, store, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}
