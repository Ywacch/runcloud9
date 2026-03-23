package store

import (
	"context"
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
		Namespace: "ganymede-test-store-" + time.Now().Format("20060102150405"),
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
