package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestHandleGetTemplates(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/templates", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type: want application/json, got %q", ct)
	}

	var list []struct {
		ID        string `json:"id"`
		Available bool   `json:"available"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 4 {
		t.Fatalf("want 4 templates, got %d", len(list))
	}
	if list[0].ID != "url-shortener" || !list[0].Available {
		t.Fatalf("first template: %+v", list[0])
	}
	if list[1].ID != "ecommerce" || !list[1].Available {
		t.Fatalf("ecommerce should be available: %+v", list[1])
	}
	if list[2].ID != "social-feed" || !list[2].Available {
		t.Fatalf("social-feed should be available: %+v", list[2])
	}
	if list[3].ID != "social-feed-pull" || !list[3].Available {
		t.Fatalf("social-feed-pull should be available: %+v", list[3])
	}
}

func TestHandleCreateDeployment_UnknownTemplate(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "not-a-real-template-id",
		Archetype: "standard",
		Nodes: []deployer.DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app", Config: map[string]interface{}{}},
			{ID: "db", Type: "db", Name: "db", Config: map[string]interface{}{}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app", TargetID: "db"},
		},
	}
	raw, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &er)
	if er.Error == "" || !strings.Contains(er.Error, "not registered") {
		t.Fatalf("error message: %q", er.Error)
	}
}

func TestHandleCreateDeployment_SimulationRecommended_PartialTopology(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "standard",
		Nodes: []deployer.DeploymentNode{
			{ID: "a", Type: "app-server", Name: "a", Config: map[string]interface{}{}},
			{ID: "b", Type: "app-server", Name: "b", Config: map[string]interface{}{}},
			{ID: "db", Type: "db", Name: "db", Config: map[string]interface{}{}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "a", TargetID: "b"},
			{SourceID: "a", TargetID: "db"},
			{SourceID: "b", TargetID: "db"},
		},
	}
	raw, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		DeploymentID string `json:"deploymentId"`
		Namespace    string `json:"namespace"`
		Status       string `json:"status"`
		Assessment   struct {
			Verdict     string `json:"verdict"`
			Explanation string `json:"explanation"`
		} `json:"assessment"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "simulation_recommended" {
		t.Fatalf("status: got %q", out.Status)
	}
	if out.DeploymentID != "" || out.Namespace != "" {
		t.Fatalf("expected empty deployment id/namespace, got %q %q", out.DeploymentID, out.Namespace)
	}
	if out.Assessment.Verdict != deployer.VerdictPartial {
		t.Fatalf("verdict: got %q", out.Assessment.Verdict)
	}
	if out.Assessment.Explanation == "" {
		t.Fatal("empty assessment explanation")
	}
}

func TestHandleCreateDeployment_UnavailableRegistryTemplate(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "ecommerce",
		Archetype: "ecommerce",
		Nodes: []deployer.DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app", Config: map[string]interface{}{}},
			{ID: "db", Type: "db", Name: "db", Config: map[string]interface{}{}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app", TargetID: "db"},
		},
	}
	raw, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status: want 201, got %d body=%s", rec.Code, rec.Body.String())
	}

	var out struct {
		DeploymentID string `json:"deploymentId"`
		Namespace    string `json:"namespace"`
		Status       string `json:"status"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "provisioning" {
		t.Fatalf("status: got %q", out.Status)
	}
	if out.DeploymentID == "" || out.Namespace == "" {
		t.Fatalf("expected deployment id/namespace, got %q / %q", out.DeploymentID, out.Namespace)
	}
}

func TestHandleCreateDeployment_ConcurrentLimit_RejectsSecondActive(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")

	s := newFakeDeploymentStore()
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "standard",
		Nodes: []deployer.DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app", Config: map[string]interface{}{}},
			{ID: "db", Type: "db", Name: "db", Config: map[string]interface{}{}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app", TargetID: "db"},
		},
	}
	raw, _ := json.Marshal(body)

	req1 := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("first create: want 201, got %d body=%s", rec1.Code, rec1.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second create: want 429, got %d body=%s", rec2.Code, rec2.Body.String())
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec2.Body.Bytes(), &er)
	if !strings.Contains(er.Error, "active deployment") {
		t.Fatalf("error message: %q", er.Error)
	}
}

func TestHandleCreateDeployment_ConcurrentLimit_RespectsEnvOverride(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("MAX_ACTIVE_DEPLOYMENTS_PER_USER", "2")

	s := newFakeDeploymentStore()
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "standard",
		Nodes: []deployer.DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app", Config: map[string]interface{}{}},
			{ID: "db", Type: "db", Name: "db", Config: map[string]interface{}{}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app", TargetID: "db"},
		},
	}
	raw, _ := json.Marshal(body)

	req1 := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("first create: want 201, got %d body=%s", rec1.Code, rec1.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusCreated {
		t.Fatalf("second create: want 201, got %d body=%s", rec2.Code, rec2.Body.String())
	}

	req3 := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req3.Header.Set("Content-Type", "application/json")
	rec3 := httptest.NewRecorder()
	mux.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusTooManyRequests {
		t.Fatalf("third create: want 429, got %d body=%s", rec3.Code, rec3.Body.String())
	}
}

func TestMaxActiveDeploymentsPerUser_DefaultAndInvalid(t *testing.T) {
	t.Setenv("MAX_ACTIVE_DEPLOYMENTS_PER_USER", "")
	if got := maxActiveDeploymentsPerUser(); got != 1 {
		t.Fatalf("default limit: got %d", got)
	}

	t.Setenv("MAX_ACTIVE_DEPLOYMENTS_PER_USER", "abc")
	if got := maxActiveDeploymentsPerUser(); got != 1 {
		t.Fatalf("invalid value fallback: got %d", got)
	}

	t.Setenv("MAX_ACTIVE_DEPLOYMENTS_PER_USER", "0")
	if got := maxActiveDeploymentsPerUser(); got != 1 {
		t.Fatalf("non-positive fallback: got %d", got)
	}
}

func TestHandleStartLoadGen_RequiresTemplate(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// Empty body => EOF => DefaultProfile with no template; nil store => cannot fill from DB.
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns-empty/loadgen", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &er)
	if !strings.Contains(er.Error, "requires a template") {
		t.Fatalf("error message: %q", er.Error)
	}
}

func TestHandleStartLoadGen_UnsupportedTemplate(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// Template id must exist in the deployment registry but have no loadgen endpoints.
	body := `{"template":"not-in-loadgen-registry","rps":10}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns1/loadgen", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &er)
	if !strings.Contains(er.Error, "no endpoint definitions") {
		t.Fatalf("error message: %q", er.Error)
	}
}

func TestHandleStartLoadGen_URLShortenerOK(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"template":"url-shortener","rps":5}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns-ok/loadgen", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Status   string `json:"status"`
		Template string `json:"template"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "started" || out.Template != "url-shortener" {
		t.Fatalf("response: %+v", out)
	}
	lg.Stop("ns-ok")
}

func TestGenerateNamespace_FormatAndUniqueness(t *testing.T) {
	t.Parallel()
	re := regexp.MustCompile(`^cloud9-[a-z0-9-]+-[a-z0-9-]+-[a-f0-9]{6}$`)
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		ns := generateNamespace("url-shortener", "dev-user")
		if !re.MatchString(ns) {
			t.Fatalf("namespace has unexpected format: %q", ns)
		}
		if len(ns) > 63 {
			t.Fatalf("namespace too long (%d): %q", len(ns), ns)
		}
		if seen[ns] {
			t.Fatalf("duplicate namespace generated: %q", ns)
		}
		seen[ns] = true
	}
}

func TestGenerateNamespace_SanitizesInputs(t *testing.T) {
	t.Parallel()
	ns := generateNamespace("Social Feed!!!", "USER_ABCDEF-1234")
	if !strings.Contains(ns, "social-feed") {
		t.Fatalf("template part not sanitized as expected: %q", ns)
	}
	if !strings.Contains(ns, "-user-abc-") {
		t.Fatalf("user part not sanitized/truncated as expected: %q", ns)
	}
}

func TestWriteDiagnosticsSummary_IncludesCollectionReason(t *testing.T) {
	t.Parallel()

	s := newFakeDeploymentStore()
	_, _ = s.CreateDeployment(context.Background(), store.CreateDeploymentParams{
		Namespace: "cloud9-test-ns",
		Template:  "url-shortener",
		Archetype: "standard",
		UserID:    "u1",
		Design:    map[string]any{},
		ExpiresAt: time.Now().Add(10 * time.Minute),
	})
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)

	h.writeDiagnosticsSummary(
		"cloud9-test-ns",
		"gs://cloud9-diagnostics/cloud9-test-ns/test.json",
		"success",
		"delete",
		nil,
		false,
		nil,
	)

	row, err := s.GetDeployment(context.Background(), "cloud9-test-ns")
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	diagRaw, ok := row.MetricsSummary["diagnostics"]
	if !ok {
		t.Fatalf("diagnostics missing from metrics summary: %+v", row.MetricsSummary)
	}
	diag, ok := diagRaw.(map[string]any)
	if !ok {
		t.Fatalf("diagnostics value has unexpected type: %T", diagRaw)
	}
	if got, _ := diag["collection_reason"].(string); got != "delete" {
		t.Fatalf("collection_reason: want delete, got %q", got)
	}
}

type fakeDeploymentStore struct {
	mu      sync.Mutex
	byNS    map[string]store.DeploymentRow
	active  []store.ActiveDeploymentRow
	nextSeq int
}

func newFakeDeploymentStore() *fakeDeploymentStore {
	return &fakeDeploymentStore{
		byNS:   make(map[string]store.DeploymentRow),
		active: make([]store.ActiveDeploymentRow, 0),
	}
}

func (f *fakeDeploymentStore) ListActiveDeployments(_ context.Context, userID string) ([]store.ActiveDeploymentRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.ActiveDeploymentRow, 0, len(f.active))
	for _, r := range f.active {
		if userID == "" {
			out = append(out, r)
			continue
		}
		row, ok := f.byNS[r.Namespace]
		if ok && row.UserID == userID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeDeploymentStore) CreateDeployment(_ context.Context, params store.CreateDeploymentParams) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSeq++
	id := fmt.Sprintf("dep-%d", f.nextSeq)
	row := store.DeploymentRow{
		ID:        id,
		UserID:    params.UserID,
		Namespace: params.Namespace,
		Template:  params.Template,
		Archetype: params.Archetype,
		Status:    "provisioning",
		Design:    params.Design,
		ExpiresAt: params.ExpiresAt,
		CreatedAt: time.Now(),
	}
	f.byNS[params.Namespace] = row
	f.active = append(f.active, store.ActiveDeploymentRow{
		ID:        id,
		Namespace: params.Namespace,
		Template:  params.Template,
		Status:    "provisioning",
		ExpiresAt: params.ExpiresAt,
		CreatedAt: row.CreatedAt,
	})
	return id, nil
}

func (f *fakeDeploymentStore) UpdateStatus(_ context.Context, namespace, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.byNS[namespace]
	if !ok {
		return nil
	}
	row.Status = status
	f.byNS[namespace] = row
	active := make([]store.ActiveDeploymentRow, 0, len(f.active))
	for _, r := range f.active {
		if r.Namespace == namespace {
			if status == "running" || status == "provisioning" {
				r.Status = status
				active = append(active, r)
			}
			continue
		}
		active = append(active, r)
	}
	f.active = active
	return nil
}

func (f *fakeDeploymentStore) UpdateExpiresAt(_ context.Context, namespace string, expiresAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.byNS[namespace]
	if ok {
		row.ExpiresAt = expiresAt
		f.byNS[namespace] = row
	}
	for i := range f.active {
		if f.active[i].Namespace == namespace {
			f.active[i].ExpiresAt = expiresAt
		}
	}
	return nil
}

func (f *fakeDeploymentStore) UpdateMetricsSummary(_ context.Context, namespace string, patch map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.byNS[namespace]
	if !ok {
		return fmt.Errorf("not found")
	}
	if row.MetricsSummary == nil {
		row.MetricsSummary = make(map[string]any)
	}
	for k, v := range patch {
		row.MetricsSummary[k] = v
	}
	f.byNS[namespace] = row
	return nil
}

func (f *fakeDeploymentStore) GetDeployment(_ context.Context, namespace string) (store.DeploymentRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.byNS[namespace]
	if !ok {
		return store.DeploymentRow{}, fmt.Errorf("not found")
	}
	return row, nil
}

func (f *fakeDeploymentStore) SetMetricsSummary(_ context.Context, namespace string, summary map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.byNS[namespace]
	if !ok {
		return fmt.Errorf("not found")
	}
	row.MetricsSummary = summary
	f.byNS[namespace] = row
	return nil
}
