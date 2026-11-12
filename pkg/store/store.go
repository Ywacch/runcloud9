package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	postgrest "github.com/supabase-community/postgrest-go"
	"github.com/supabase-community/supabase-go"
)

// RequestObserver is invoked once per Supabase PostgREST call in the store.
type RequestObserver func(op string)

// DeploymentStore wraps Supabase client for cloud_deployments table operations.
type DeploymentStore struct {
	client          *supabase.Client
	requestObserver RequestObserver
}

// SetRequestObserver registers a callback for Supabase-bound store methods.
// The parameter is an unnamed function type so a caller in another package can
// require this method on an interface. A defined type such as RequestObserver
// does not match func(string) in a method set.
func (s *DeploymentStore) SetRequestObserver(fn func(string)) {
	if s == nil {
		return
	}
	s.requestObserver = fn
}

func (s *DeploymentStore) observe(op string) {
	if s != nil && s.requestObserver != nil {
		s.requestObserver(op)
	}
}

// ActiveDeploymentRow is a lightweight view used by the "rejoin" flow.
// It intentionally omits the Design column so list queries stay cheap.
type ActiveDeploymentRow struct {
	ID        string    `json:"id"`
	Namespace string    `json:"namespace"`
	Template  string    `json:"template"`
	Status    string    `json:"status"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

// DeploymentHistoryRow is a lightweight completed-deployment view for profile history.
// MetricsSummary is included so the API can expose selected summary scalars without
// returning the full session history.
type DeploymentHistoryRow struct {
	ID             string                     `json:"id"`
	Namespace      string                     `json:"namespace"`
	Template       string                     `json:"template"`
	Archetype      string                     `json:"archetype"`
	Status         string                     `json:"status"`
	MetricsSummary map[string]any             `json:"metrics_summary"`
	ExpiresAt      time.Time                  `json:"expires_at"`
	CreatedAt      time.Time                  `json:"created_at"`
	Topology       *DeploymentHistoryTopology `json:"topology,omitempty"`
}

// DeploymentHistoryTopology is a compact layout for history miniview cards.
type DeploymentHistoryTopology struct {
	Nodes       []DeploymentHistoryTopologyNode `json:"nodes"`
	Connections []DeploymentHistoryTopologyConn `json:"connections"`
}

type DeploymentHistoryTopologyNode struct {
	ID       string    `json:"id"`
	Type     string    `json:"type"`
	Position []float64 `json:"position,omitempty"`
}

type DeploymentHistoryTopologyConn struct {
	SourceID string `json:"sourceId"`
	TargetID string `json:"targetId"`
	Protocol string `json:"protocol"`
}

type deploymentHistoryDBRow struct {
	ID             string         `json:"id"`
	Namespace      string         `json:"namespace"`
	Template       string         `json:"template"`
	Archetype      string         `json:"archetype"`
	Status         string         `json:"status"`
	MetricsSummary map[string]any `json:"metrics_summary"`
	ExpiresAt      time.Time      `json:"expires_at"`
	CreatedAt      time.Time      `json:"created_at"`
	Design         map[string]any `json:"design"`
}

// CreateDeploymentParams are the inputs for creating a deployment row.
type CreateDeploymentParams struct {
	Namespace     string
	Template      string
	Archetype     string
	Design        map[string]any
	ExpiresAt     time.Time
	UserID        string
	AdmittedCPUm  int64 // cluster admission weight; 0 allowed
	AdmittedMemMi int64
	// Status defaults to "provisioning" when empty. Use "queued" for the deploy queue.
	Status       string
	ComparisonID string // empty for solo runs
	RunRole      string // "", "baseline", "candidate"
}

// QueuedDeploymentRow is a FIFO queue entry with persisted admission weights.
type QueuedDeploymentRow struct {
	ID            string    `json:"id"`
	UserID        string    `json:"user_id"`
	Namespace     string    `json:"namespace"`
	Template      string    `json:"template"`
	Status        string    `json:"status"`
	ExpiresAt     time.Time `json:"expires_at"`
	CreatedAt     time.Time `json:"created_at"`
	AdmittedCPUm  *int64    `json:"admitted_cpu_m"`
	AdmittedMemMi *int64    `json:"admitted_mem_mi"`
}

// AdmissionOccupancy is the summed admitted weight of active deployments.
type AdmissionOccupancy struct {
	CPUMillicores int64
	MemoryMi      int64
}

// DeploymentRow represents a row from cloud_deployments (subset of columns).
type DeploymentRow struct {
	ID             string         `json:"id"`
	UserID         string         `json:"user_id"`
	Namespace      string         `json:"namespace"`
	Template       string         `json:"template"`
	Archetype      string         `json:"archetype"`
	Status         string         `json:"status"`
	Design         map[string]any `json:"design"`
	MetricsSummary map[string]any `json:"metrics_summary"`
	ExpiresAt      time.Time      `json:"expires_at"`
	CreatedAt      time.Time      `json:"created_at"`
	ComparisonID   *string        `json:"comparison_id"`
	RunRole        *string        `json:"run_role"`
}

// NewDeploymentStore creates a store using the Supabase URL and service role key.
func NewDeploymentStore(supabaseURL, serviceRoleKey string) (*DeploymentStore, error) {
	client, err := supabase.NewClient(supabaseURL, serviceRoleKey, nil)
	if err != nil {
		return nil, fmt.Errorf("store: new client: %w", err)
	}
	return &DeploymentStore{client: client}, nil
}

// SupabaseClient exposes the client for bootstrap code (e.g. premium template registry).
func (s *DeploymentStore) SupabaseClient() *supabase.Client {
	if s == nil {
		return nil
	}
	return s.client
}

// CreateDeployment inserts a new deployment row and returns the new row ID.
// Status defaults to "provisioning" when params.Status is empty.
func (s *DeploymentStore) CreateDeployment(ctx context.Context, params CreateDeploymentParams) (string, error) {
	status := params.Status
	if status == "" {
		status = "provisioning"
	}
	row, err := DeploymentInsertRow(params, status)
	if err != nil {
		return "", err
	}
	s.observe("create_deployment")
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
	s.observe("update_status")
	_, _, err := s.client.From("cloud_deployments").
		Update(map[string]any{"status": status}, "", "").
		Eq("namespace", namespace).
		Execute()
	if err != nil {
		return fmt.Errorf("store.UpdateStatus: %w", err)
	}
	return nil
}

// ClaimStatus sets status to `to` only when the row is still one of `from`.
// It returns false when another writer already changed the row.
func (s *DeploymentStore) ClaimStatus(ctx context.Context, namespace string, from []string, to string) (bool, error) {
	if namespace == "" || to == "" || len(from) == 0 {
		return false, fmt.Errorf("store.ClaimStatus: namespace, from, and to are required")
	}
	s.observe("claim_status")
	data, _, err := s.client.From("cloud_deployments").
		Update(map[string]any{"status": to}, "representation", "").
		Eq("namespace", namespace).
		In("status", from).
		Execute()
	if err != nil {
		return false, fmt.Errorf("store.ClaimStatus: %w", err)
	}
	var rows []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return false, fmt.Errorf("store.ClaimStatus: parse: %w", err)
	}
	return len(rows) > 0, nil
}

// ListDeploymentsByStatus returns rows whose status is in statuses.
func (s *DeploymentStore) ListDeploymentsByStatus(ctx context.Context, statuses []string) ([]DeploymentRow, error) {
	if len(statuses) == 0 {
		return nil, fmt.Errorf("store.ListDeploymentsByStatus: statuses are required")
	}
	s.observe("list_deployments_by_status")
	data, _, err := s.client.From("cloud_deployments").
		Select("id,namespace,template,archetype,status,expires_at,created_at", "exact", false).
		In("status", statuses).
		Execute()
	if err != nil {
		return nil, fmt.Errorf("store.ListDeploymentsByStatus: %w", err)
	}
	var rows []DeploymentRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("store.ListDeploymentsByStatus: parse: %w", err)
	}
	return rows, nil
}

// SetMetricsSummary writes the metrics_summary JSONB column for the given namespace.
func (s *DeploymentStore) SetMetricsSummary(ctx context.Context, namespace string, summary map[string]any) error {
	s.observe("set_metrics_summary")
	_, _, err := s.client.From("cloud_deployments").
		Update(map[string]any{"metrics_summary": summary}, "", "").
		Eq("namespace", namespace).
		Execute()
	if err != nil {
		return fmt.Errorf("store.SetMetricsSummary: %w", err)
	}
	return nil
}

// UpdateMetricsSummary patches the metrics_summary JSONB column for a deployment.
func (s *DeploymentStore) UpdateMetricsSummary(ctx context.Context, namespace string, summary map[string]any) error {
	if summary == nil {
		summary = map[string]any{}
	}
	s.observe("update_metrics_summary")
	_, _, err := s.client.From("cloud_deployments").
		Update(map[string]any{"metrics_summary": summary}, "", "").
		Eq("namespace", namespace).
		Execute()
	if err != nil {
		return fmt.Errorf("store.UpdateMetricsSummary: %w", err)
	}
	return nil
}

// UpdateExpiresAt updates only the expires_at column for the given namespace.
func (s *DeploymentStore) UpdateExpiresAt(ctx context.Context, namespace string, expiresAt time.Time) error {
	s.observe("update_expires_at")
	_, _, err := s.client.From("cloud_deployments").
		Update(map[string]any{"expires_at": expiresAt.Format(time.RFC3339)}, "", "").
		Eq("namespace", namespace).
		Execute()
	if err != nil {
		return fmt.Errorf("store.UpdateExpiresAt: %w", err)
	}
	return nil
}

// GetStaleDeployments returns rows with status IN ('running','provisioning') for recovery.
// Caller uses ExpiresAt to either fire teardown now or re-register a timer.
func (s *DeploymentStore) GetStaleDeployments(ctx context.Context) ([]DeploymentRow, error) {
	s.observe("get_stale_deployments")
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
	s.observe("get_deployment")
	data, _, err := s.client.From("cloud_deployments").
		Select("id,user_id,namespace,template,archetype,status,design,metrics_summary,expires_at,created_at,comparison_id,run_role", "exact", false).
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

// ListActiveDeployments returns deployments with status IN
// ('running','provisioning','queued','terminating').
// Queued counts toward per-user concurrency so users cannot stuff the queue.
// Terminating still holds a namespace, so it counts toward the same cap.
// If userID is non-empty, it filters by user_id.
func (s *DeploymentStore) ListActiveDeployments(ctx context.Context, userID string) ([]ActiveDeploymentRow, error) {
	q := s.client.From("cloud_deployments").
		Select("id,namespace,template,status,expires_at,created_at", "exact", false).
		In("status", []string{"running", "provisioning", "queued", "terminating"})

	if userID != "" {
		q = q.Eq("user_id", userID)
	}

	s.observe("list_active_deployments")
	data, _, err := q.Execute()
	if err != nil {
		return nil, fmt.Errorf("store.ListActiveDeployments: %w", err)
	}

	var rows []ActiveDeploymentRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("store.ListActiveDeployments: parse: %w", err)
	}

	return rows, nil
}

// ListActiveForRegistry returns full deployment rows for active statuses in one query.
func (s *DeploymentStore) ListActiveForRegistry(ctx context.Context) ([]DeploymentRow, error) {
	s.observe("list_active_for_registry")
	data, _, err := s.client.From("cloud_deployments").
		Select("id,user_id,namespace,template,archetype,status,design,metrics_summary,expires_at,created_at,comparison_id,run_role", "exact", false).
		In("status", []string{"running", "provisioning", "queued", "terminating"}).
		Execute()
	if err != nil {
		return nil, fmt.Errorf("store.ListActiveForRegistry: %w", err)
	}
	var rows []DeploymentRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("store.ListActiveForRegistry: parse: %w", err)
	}
	return rows, nil
}

// ListQueuedDeployments returns all queued rows oldest-first (global FIFO).
func (s *DeploymentStore) ListQueuedDeployments(ctx context.Context) ([]QueuedDeploymentRow, error) {
	s.observe("list_queued_deployments")
	data, _, err := s.client.From("cloud_deployments").
		Select("id,user_id,namespace,template,status,expires_at,created_at,admitted_cpu_m,admitted_mem_mi", "exact", false).
		Eq("status", "queued").
		Order("created_at", &postgrest.OrderOpts{Ascending: true}).
		Execute()
	if err != nil {
		return nil, fmt.Errorf("store.ListQueuedDeployments: %w", err)
	}
	var rows []QueuedDeploymentRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("store.ListQueuedDeployments: parse: %w", err)
	}
	return rows, nil
}

// ClaimQueued promotes a row from queued → provisioning when it is still queued.
// Returns true if this caller claimed the row.
func (s *DeploymentStore) ClaimQueued(ctx context.Context, namespace string) (bool, error) {
	s.observe("claim_queued")
	data, _, err := s.client.From("cloud_deployments").
		Update(map[string]any{"status": "provisioning"}, "representation", "").
		Eq("namespace", namespace).
		Eq("status", "queued").
		Execute()
	if err != nil {
		return false, fmt.Errorf("store.ClaimQueued: %w", err)
	}
	var rows []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return false, fmt.Errorf("store.ClaimQueued: parse: %w", err)
	}
	return len(rows) > 0, nil
}

// CountQueuedDeployments returns the number of rows with status=queued.
func (s *DeploymentStore) CountQueuedDeployments(ctx context.Context) (int, error) {
	s.observe("count_queued_deployments")
	_, count, err := s.client.From("cloud_deployments").
		Select("id", "exact", true).
		Eq("status", "queued").
		Execute()
	if err != nil {
		return 0, fmt.Errorf("store.CountQueuedDeployments: %w", err)
	}
	return int(count), nil
}

// CountUserDeploymentsSince returns how many of the user's deployments this month
// actually reached live (status=running, or metrics_summary.running_at set after
// a successful provision). Queued/cancelled/failed-before-running do not count.
func (s *DeploymentStore) CountUserDeploymentsSince(ctx context.Context, userID string, since time.Time) (int, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return 0, fmt.Errorf("store.CountUserDeploymentsSince: user ID is required")
	}
	s.observe("count_user_deployments_since")
	_, count, err := s.client.From("cloud_deployments").
		Select("id", "exact", true).
		Eq("user_id", userID).
		Filter("created_at", "gte", since.UTC().Format(time.RFC3339)).
		Or("status.eq.running,metrics_summary->>running_at.not.is.null", "").
		Execute()
	if err != nil {
		return 0, fmt.Errorf("store.CountUserDeploymentsSince: %w", err)
	}
	return int(count), nil
}

// SumActiveAdmissionWeights sums admitted_cpu_m / admitted_mem_mi for rows with
// status IN ('provisioning','running','terminating'). NULL weights count as 0.
// Terminating stays in the sum until the namespace is gone.
func (s *DeploymentStore) SumActiveAdmissionWeights(ctx context.Context) (AdmissionOccupancy, error) {
	s.observe("sum_active_admission_weights")
	data, _, err := s.client.From("cloud_deployments").
		Select("admitted_cpu_m,admitted_mem_mi", "exact", false).
		In("status", []string{"running", "provisioning", "terminating"}).
		Execute()
	if err != nil {
		return AdmissionOccupancy{}, fmt.Errorf("store.SumActiveAdmissionWeights: %w", err)
	}
	var rows []struct {
		AdmittedCPUm  *int64 `json:"admitted_cpu_m"`
		AdmittedMemMi *int64 `json:"admitted_mem_mi"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return AdmissionOccupancy{}, fmt.Errorf("store.SumActiveAdmissionWeights: parse: %w", err)
	}
	var occ AdmissionOccupancy
	for _, r := range rows {
		if r.AdmittedCPUm != nil {
			occ.CPUMillicores += *r.AdmittedCPUm
		}
		if r.AdmittedMemMi != nil {
			occ.MemoryMi += *r.AdmittedMemMi
		}
	}
	return occ, nil
}

// ListUserDeploymentHistory returns a user's terminated and failed deployments,
// newest first. It rejects an empty user ID so this history query can never
// accidentally become an unscoped service-role query.
func (s *DeploymentStore) ListUserDeploymentHistory(ctx context.Context, userID string, limit int) ([]DeploymentHistoryRow, error) {
	if strings.TrimSpace(userID) == "" {
		return nil, fmt.Errorf("store.ListUserDeploymentHistory: user ID is required")
	}
	if limit < 1 {
		limit = 20
	}
	if limit > 50 {
		limit = 50
	}

	s.observe("list_user_deployment_history")
	data, _, err := s.client.From("cloud_deployments").
		Select("id,namespace,template,archetype,status,metrics_summary,expires_at,created_at,design", "exact", false).
		Eq("user_id", userID).
		In("status", []string{"terminated", "failed"}).
		Order("created_at", &postgrest.OrderOpts{Ascending: false}).
		Limit(limit, "").
		Execute()
	if err != nil {
		return nil, fmt.Errorf("store.ListUserDeploymentHistory: %w", err)
	}

	var dbRows []deploymentHistoryDBRow
	if err := json.Unmarshal(data, &dbRows); err != nil {
		return nil, fmt.Errorf("store.ListUserDeploymentHistory: parse: %w", err)
	}
	rows := make([]DeploymentHistoryRow, 0, len(dbRows))
	for _, dbRow := range dbRows {
		rows = append(rows, DeploymentHistoryRow{
			ID:             dbRow.ID,
			Namespace:      dbRow.Namespace,
			Template:       dbRow.Template,
			Archetype:      dbRow.Archetype,
			Status:         dbRow.Status,
			MetricsSummary: dbRow.MetricsSummary,
			ExpiresAt:      dbRow.ExpiresAt,
			CreatedAt:      dbRow.CreatedAt,
			Topology:       CompactTopologyFromDesign(dbRow.Design),
		})
	}
	return rows, nil
}

// CompactTopologyFromDesign extracts id/type/position nodes and connections for history cards.
func CompactTopologyFromDesign(design map[string]any) *DeploymentHistoryTopology {
	if design == nil {
		return nil
	}
	nodesRaw, ok := design["nodes"].([]any)
	if !ok || len(nodesRaw) == 0 {
		return nil
	}
	nodes := make([]DeploymentHistoryTopologyNode, 0, len(nodesRaw))
	for _, raw := range nodesRaw {
		nodeMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, _ := nodeMap["id"].(string)
		typ, _ := nodeMap["type"].(string)
		if id == "" || typ == "" {
			continue
		}
		out := DeploymentHistoryTopologyNode{ID: id, Type: typ}
		if posRaw, ok := nodeMap["position"].([]any); ok && len(posRaw) == 3 {
			pos := make([]float64, 0, 3)
			valid := true
			for _, v := range posRaw {
				switch n := v.(type) {
				case float64:
					pos = append(pos, n)
				case float32:
					pos = append(pos, float64(n))
				case int:
					pos = append(pos, float64(n))
				case int64:
					pos = append(pos, float64(n))
				case json.Number:
					f, err := n.Float64()
					if err != nil {
						valid = false
						break
					}
					pos = append(pos, f)
				default:
					valid = false
				}
				if !valid {
					break
				}
			}
			if valid && len(pos) == 3 {
				out.Position = pos
			}
		}
		nodes = append(nodes, out)
	}
	if len(nodes) == 0 {
		return nil
	}
	connsRaw, _ := design["connections"].([]any)
	connections := make([]DeploymentHistoryTopologyConn, 0, len(connsRaw))
	for _, raw := range connsRaw {
		connMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		sourceID, _ := connMap["sourceId"].(string)
		targetID, _ := connMap["targetId"].(string)
		if sourceID == "" || targetID == "" {
			continue
		}
		protocol, _ := connMap["protocol"].(string)
		if protocol == "" {
			protocol = "http"
		}
		connections = append(connections, DeploymentHistoryTopologyConn{
			SourceID: sourceID,
			TargetID: targetID,
			Protocol: protocol,
		})
	}
	return &DeploymentHistoryTopology{
		Nodes:       nodes,
		Connections: connections,
	}
}
