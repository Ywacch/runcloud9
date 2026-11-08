package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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
		if liveStoreCredentialsInvalid(err) {
			t.Skipf("live Supabase credentials rejected: %v", err)
		}
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

func TestLiveStoreCredentialsInvalid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "unregistered", err: errString("store.CreateDeployment: () Unregistered API key"), want: true},
		{name: "invalid key", err: errString("Invalid API key"), want: true},
		{name: "logic error", err: errString("store.CreateDeployment: parse response"), want: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := liveStoreCredentialsInvalid(tt.err); got != tt.want {
				t.Fatalf("liveStoreCredentialsInvalid(%v) = %v want %v", tt.err, got, tt.want)
			}
		})
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func liveStoreCredentialsInvalid(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unregistered api key") ||
		strings.Contains(msg, "invalid api key") ||
		(strings.Contains(msg, "jwt") && strings.Contains(msg, "unauthorized"))
}

func TestListUserDeploymentHistory_requiresUser(t *testing.T) {
	t.Parallel()

	s := &DeploymentStore{}
	if _, err := s.ListUserDeploymentHistory(context.Background(), " ", 20); err == nil {
		t.Fatal("expected empty user ID to fail")
	}
}

func TestStore_ObservesGetDeployment(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/v1/cloud_deployments" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]DeploymentRow{
			{
				ID:        "dep-1",
				Namespace: "cloud9-user-1",
				Template:  "url-shortener",
				Status:    "running",
			},
		})
	}))
	defer server.Close()

	s, err := NewDeploymentStore(server.URL, "service-key")
	if err != nil {
		t.Fatalf("NewDeploymentStore: %v", err)
	}

	var ops []string
	s.SetRequestObserver(func(op string) {
		ops = append(ops, op)
	})

	_, err = s.GetDeployment(context.Background(), "cloud9-user-1")
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if len(ops) != 1 || ops[0] != "get_deployment" {
		t.Fatalf("observer ops = %v, want [get_deployment]", ops)
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

func TestSumActiveAdmissionWeights(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/v1/cloud_deployments" {
			t.Errorf("path = %q", r.URL.Path)
		}
		q := r.URL.Query()
		if got := q.Get("status"); got != "in.(running,provisioning,terminating)" {
			t.Errorf("status filter = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		cpuA, cpuB := int64(100), int64(200)
		memA := int64(10)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"admitted_cpu_m": cpuA, "admitted_mem_mi": memA},
			{"admitted_cpu_m": cpuB, "admitted_mem_mi": nil},
			{"admitted_cpu_m": nil, "admitted_mem_mi": nil},
		})
	}))
	defer server.Close()

	s, err := NewDeploymentStore(server.URL, "service-key")
	if err != nil {
		t.Fatalf("NewDeploymentStore: %v", err)
	}
	occ, err := s.SumActiveAdmissionWeights(context.Background())
	if err != nil {
		t.Fatalf("SumActiveAdmissionWeights: %v", err)
	}
	if occ.CPUMillicores != 300 || occ.MemoryMi != 10 {
		t.Fatalf("occupancy = %+v, want cpu=300 mem=10", occ)
	}
}

func TestCreateDeployment_IncludesAdmittedWeights(t *testing.T) {
	t.Parallel()

	var inserted []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&inserted); err != nil {
			t.Fatalf("decode insert body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]string{{"id": "dep-1"}})
	}))
	defer server.Close()

	s, err := NewDeploymentStore(server.URL, "service-key")
	if err != nil {
		t.Fatalf("NewDeploymentStore: %v", err)
	}
	_, err = s.CreateDeployment(context.Background(), CreateDeploymentParams{
		Namespace:     "ns-1",
		Template:      "url-shortener",
		Archetype:     "standard",
		Design:        map[string]any{},
		ExpiresAt:     time.Now().Add(time.Hour),
		AdmittedCPUm:  925,
		AdmittedMemMi: 2496,
		Status:        "queued",
	})
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}
	if len(inserted) != 1 {
		t.Fatalf("inserted rows = %d", len(inserted))
	}
	assertJSONInt64(t, inserted[0]["admitted_cpu_m"], 925)
	assertJSONInt64(t, inserted[0]["admitted_mem_mi"], 2496)
	if inserted[0]["status"] != "queued" {
		t.Fatalf("status = %v, want queued", inserted[0]["status"])
	}
}

func TestCreateDeployment_InvalidRunRole(t *testing.T) {
	t.Parallel()

	cmp := "550e8400-e29b-41d4-a716-446655440000"
	base := CreateDeploymentParams{
		Namespace: "ns-bad",
		Template:  "url-shortener",
		ExpiresAt: time.Now().Add(time.Hour),
	}
	cases := []struct {
		name   string
		params CreateDeploymentParams
		substr string
	}{
		{
			name: "bad role",
			params: func() CreateDeploymentParams {
				p := base
				p.ComparisonID = cmp
				p.RunRole = "solo"
				return p
			}(),
			substr: "invalid run role",
		},
		{
			name: "role without id",
			params: func() CreateDeploymentParams {
				p := base
				p.RunRole = RunRoleCandidate
				return p
			}(),
			substr: "requires comparison_id",
		},
		{
			name: "id without role",
			params: func() CreateDeploymentParams {
				p := base
				p.ComparisonID = cmp
				return p
			}(),
			substr: "requires run_role",
		},
	}
	s, err := NewDeploymentStore("http://unused", "service-key")
	if err != nil {
		t.Fatalf("NewDeploymentStore: %v", err)
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := s.CreateDeployment(context.Background(), tc.params)
			if err == nil || !strings.Contains(err.Error(), tc.substr) {
				t.Fatalf("CreateDeployment err = %v, want substring %q", err, tc.substr)
			}
		})
	}
}

func TestListActiveDeployments_IncludesQueued(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if got := q.Get("status"); got != "in.(running,provisioning,queued,terminating)" {
			t.Errorf("status filter = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer server.Close()

	s, err := NewDeploymentStore(server.URL, "service-key")
	if err != nil {
		t.Fatalf("NewDeploymentStore: %v", err)
	}
	_, err = s.ListActiveDeployments(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("ListActiveDeployments: %v", err)
	}
}

func TestCountUserDeploymentsSince_Query(t *testing.T) {
	t.Parallel()

	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assertQueryValue(t, map[string][]string(q), "user_id", "eq.user-1")
		assertQueryValue(t, map[string][]string(q), "created_at", "gte."+since.Format(time.RFC3339))
		if got := q.Get("or"); got == "" {
			t.Error("expected or= filter for successful sessions")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer server.Close()

	s, err := NewDeploymentStore(server.URL, "service-key")
	if err != nil {
		t.Fatalf("NewDeploymentStore: %v", err)
	}
	if _, err := s.CountUserDeploymentsSince(context.Background(), "user-1", since); err != nil {
		t.Fatalf("CountUserDeploymentsSince: %v", err)
	}
}

func assertJSONInt64(t *testing.T, v any, want int64) {
	t.Helper()
	switch n := v.(type) {
	case float64:
		if int64(n) != want {
			t.Fatalf("got %v want %d", n, want)
		}
	case int64:
		if n != want {
			t.Fatalf("got %d want %d", n, want)
		}
	case json.Number:
		i, err := n.Int64()
		if err != nil || i != want {
			t.Fatalf("got %v want %d err=%v", n, want, err)
		}
	default:
		t.Fatalf("unexpected type %T = %v", v, v)
	}
}
