package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestCreateAndUpdateDeployment(t *testing.T) {
	url := os.Getenv("SUPABASE_URL")
	key := os.Getenv("SUPABASE_SERVICE_KEY")
	if url == "" || key == "" {
		t.Skip("SUPABASE_URL and SUPABASE_SERVICE_KEY required for store test")
	}

	s, err := NewDeploymentStore(url, key)
	if err != nil {
		t.Fatalf("NewDeploymentStore: %v", err)
	}
	ctx := context.Background()

	params := CreateDeploymentParams{
		Namespace: "cloud9-test-store-" + time.Now().Format("20060102150405"),
		Template:  "url-shortener",
		Archetype: "browse-heavy",
		Design:    map[string]any{"version": "1.0", "nodes": []any{}},
		ExpiresAt: time.Now().Add(1 * time.Hour),
		UserID:    "",
	}

	id, err := s.CreateDeployment(ctx, params)
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty id")
	}

	err = s.UpdateStatus(ctx, params.Namespace, "running")
	if err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	row, err := s.GetDeployment(ctx, params.Namespace)
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if row.Status != "running" {
		t.Errorf("expected status running, got %s", row.Status)
	}
}

func TestListUserDeploymentHistory_requiresUser(t *testing.T) {
	t.Parallel()

	s := &DeploymentStore{}
	if _, err := s.ListUserDeploymentHistory(context.Background(), " ", 20); err == nil {
		t.Fatal("expected empty user ID to fail")
	}
}

func TestListUserDeploymentHistory(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, time.July, 18, 10, 0, 0, 0, time.UTC)
	expiresAt := createdAt.Add(10 * time.Minute)
	queryValues := make(chan map[string][]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/v1/cloud_deployments" {
			t.Errorf("path = %q, want /rest/v1/cloud_deployments", r.URL.Path)
		}
		queryValues <- r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]DeploymentHistoryRow{
			{
				ID:        "dep-1",
				Namespace: "cloud9-user-1",
				Template:  "url-shortener",
				Archetype: "browse-heavy",
				Status:    "terminated",
				MetricsSummary: map[string]any{
					"peak_rps": 1200.0,
				},
				ExpiresAt: expiresAt,
				CreatedAt: createdAt,
			},
		})
	}))
	defer server.Close()

	s, err := NewDeploymentStore(server.URL, "service-key")
	if err != nil {
		t.Fatalf("NewDeploymentStore: %v", err)
	}

	rows, err := s.ListUserDeploymentHistory(context.Background(), "user-1", 500)
	if err != nil {
		t.Fatalf("ListUserDeploymentHistory: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != "dep-1" {
		t.Fatalf("rows = %#v", rows)
	}

	query := <-queryValues
	assertQueryValue(t, query, "user_id", "eq.user-1")
	assertQueryValue(t, query, "status", "in.(terminated,failed)")
	assertQueryValue(t, query, "order", "created_at.desc.nullslast")
	assertQueryValue(t, query, "limit", "50")
}

func assertQueryValue(t *testing.T, values map[string][]string, key, want string) {
	t.Helper()
	got := ""
	if entries := values[key]; len(entries) > 0 {
		got = entries[0]
	}
	if got != want {
		t.Fatalf("query %s = %q, want %q", key, got, want)
	}
}
