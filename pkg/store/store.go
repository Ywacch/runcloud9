package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/supabase-community/supabase-go"
)

// DeploymentStore wraps Supabase client for cloud_deployments table operations.
type DeploymentStore struct {
	client *supabase.Client
}

// CreateDeploymentParams are the inputs for creating a deployment row.
type CreateDeploymentParams struct {
	Namespace string
	Template  string
	Archetype string
	Design    map[string]any
	ExpiresAt time.Time
	UserID    string
}

// DeploymentRow represents a row from cloud_deployments (subset of columns).
type DeploymentRow struct {
	ID             string         `json:"id"`
	Namespace      string         `json:"namespace"`
	Template       string         `json:"template"`
	Archetype      string         `json:"archetype"`
	Status         string         `json:"status"`
	Design         map[string]any `json:"design"`
	MetricsSummary map[string]any `json:"metrics_summary"`
	ExpiresAt      time.Time      `json:"expires_at"`
	CreatedAt      time.Time      `json:"created_at"`
}

// NewDeploymentStore creates a store using the Supabase URL and service role key.
func NewDeploymentStore(supabaseURL, serviceRoleKey string) (*DeploymentStore, error) {
	client, err := supabase.NewClient(supabaseURL, serviceRoleKey, nil)
	if err != nil {
		return nil, fmt.Errorf("store: new client: %w", err)
	}
	return &DeploymentStore{client: client}, nil
}

// CreateDeployment inserts a new row with status='provisioning' and returns the new row ID.
func (s *DeploymentStore) CreateDeployment(ctx context.Context, params CreateDeploymentParams) (string, error) {
	userIDVal := any(params.UserID)
	if params.UserID == "" {
		userIDVal = nil
	}
	row := map[string]any{
		"namespace":  params.Namespace,
		"template":   params.Template,
		"archetype":  params.Archetype,
		"status":     "provisioning",
		"design":     params.Design,
		"expires_at": params.ExpiresAt.Format(time.RFC3339),
		"user_id":    userIDVal,
	}
	data, _, err := s.client.From("cloud_deployments").Insert([]map[string]any{row}, false, "", "representation", "exact").Execute()
	if err != nil {
		return "", fmt.Errorf("store.CreateDeployment: %w", err)
	}
	var result []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &result); err != nil || len(result) == 0 {
		return "", fmt.Errorf("store.CreateDeployment: parse response: %w", err)
	}
	return result[0].ID, nil
}

// UpdateStatus updates only the status column for the given namespace.
func (s *DeploymentStore) UpdateStatus(ctx context.Context, namespace, status string) error {
	_, _, err := s.client.From("cloud_deployments").
		Update(map[string]any{"status": status}, "", "").
		Eq("namespace", namespace).
		Execute()
	if err != nil {
		return fmt.Errorf("store.UpdateStatus: %w", err)
	}
	return nil
}

// SetMetricsSummary writes the metrics_summary JSONB column for the given namespace.
func (s *DeploymentStore) SetMetricsSummary(ctx context.Context, namespace string, summary map[string]any) error {
	_, _, err := s.client.From("cloud_deployments").
		Update(map[string]any{"metrics_summary": summary}, "", "").
		Eq("namespace", namespace).
		Execute()
	if err != nil {
		return fmt.Errorf("store.SetMetricsSummary: %w", err)
	}
	return nil
}

// GetStaleDeployments returns rows with status IN ('running','provisioning') for recovery.
// Caller uses ExpiresAt to either fire teardown now or re-register a timer.
func (s *DeploymentStore) GetStaleDeployments(ctx context.Context) ([]DeploymentRow, error) {
	data, _, err := s.client.From("cloud_deployments").
		Select("id,namespace,template,archetype,status,expires_at,created_at", "exact", false).
		In("status", []string{"running", "provisioning"}).
		Execute()
	if err != nil {
		return nil, fmt.Errorf("store.GetStaleDeployments: %w", err)
	}
	var rows []DeploymentRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("store.GetStaleDeployments: parse: %w", err)
	}
	return rows, nil
}

// GetDeployment returns the single row matching the namespace.
func (s *DeploymentStore) GetDeployment(ctx context.Context, namespace string) (DeploymentRow, error) {
	data, _, err := s.client.From("cloud_deployments").
		Select("id,namespace,template,archetype,status,design,metrics_summary,expires_at,created_at", "exact", false).
		Eq("namespace", namespace).
		Limit(1, "").
		Execute()
	if err != nil {
		return DeploymentRow{}, fmt.Errorf("store.GetDeployment: %w", err)
	}
	var rows []DeploymentRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return DeploymentRow{}, fmt.Errorf("store.GetDeployment: parse: %w", err)
	}
	if len(rows) == 0 {
		return DeploymentRow{}, fmt.Errorf("store.GetDeployment: not found")
	}
	return rows[0], nil
}
