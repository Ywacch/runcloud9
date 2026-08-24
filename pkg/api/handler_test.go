package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
	"github.com/ywacch/cloud9/pkg/templates"
)

type fakeBlockingDeployer struct {
	deployFn   func(ctx context.Context, cfg deployer.DeployConfig) error
	teardownFn func(ctx context.Context, namespace string) error
}

func (f *fakeBlockingDeployer) Deploy(ctx context.Context, cfg deployer.DeployConfig) error {
	if f.deployFn != nil {
		return f.deployFn(ctx, cfg)
	}
	return nil
}

func (f *fakeBlockingDeployer) Teardown(ctx context.Context, namespace string) error {
	if f.teardownFn != nil {
		return f.teardownFn(ctx, namespace)
	}
	return nil
}

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
		ID             string `json:"id"`
		Available      bool   `json:"available"`
		SupportsWorker bool   `json:"supportsWorker"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 4 {
		t.Fatalf("want 4 templates, got %d", len(list))
	}
	if list[0].ID != "url-shortener" || !list[0].Available || list[0].SupportsWorker {
		t.Fatalf("url-shortener: %+v", list[0])
	}
	if list[1].ID != "ecommerce" || !list[1].Available || !list[1].SupportsWorker {
		t.Fatalf("ecommerce: %+v", list[1])
	}
	if list[2].ID != "social-feed" || !list[2].Available || !list[2].SupportsWorker {
		t.Fatalf("social-feed: %+v", list[2])
	}
	if list[3].ID != "social-feed-pull" || !list[3].Available || list[3].SupportsWorker {
		t.Fatalf("social-feed-pull: %+v", list[3])
	}
}

func TestHandleGetResourceProfiles(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/meta/resource-profiles", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type: want application/json, got %q", ct)
	}

	var resp deployer.ResourceCatalogResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Profiles["small"].CPU != "125m" {
		t.Fatalf("small cpu: %+v", resp.Profiles["small"])
	}
	if resp.Components["mq"].CPU != "200m" {
		t.Fatalf("mq cpu: %+v", resp.Components["mq"])
	}
	if resp.Components["lb"].Memory != "128Mi" {
		t.Fatalf("lb memory: %+v", resp.Components["lb"])
	}
}

func TestHandleGetTopologyRules(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/meta/topology-rules", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type: want application/json, got %q", ct)
	}

	var resp deployer.TopologyRulesResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Connections) != 36 {
		t.Fatalf("connections: want 36, got %d", len(resp.Connections))
	}
	if len(resp.Singletons) != 5 {
		t.Fatalf("singletons: %+v", resp.Singletons)
	}
	foundWorkerReq := false
	foundCacheSingleton := false
	for _, s := range resp.Singletons {
		if s.NodeType == "cache" {
			foundCacheSingleton = true
			if s.ScaleWithReplicas {
				t.Fatal("cache must not scaleWithReplicas")
			}
		}
		if s.NodeType == "app-server" && !s.ScaleWithReplicas {
			t.Fatal("app-server must scaleWithReplicas")
		}
	}
	if !foundCacheSingleton {
		t.Fatal("expected cache singleton in topology-rules")
	}
	for _, r := range resp.RequiredOutbound {
		if r.NodeType == "worker" {
			foundWorkerReq = true
			break
		}
	}
	if !foundWorkerReq {
		t.Fatalf("expected worker requiredOutbound, got %+v", resp.RequiredOutbound)
	}
}

func TestHandleCreateDeployment_ResourceBudgetExceeded(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "url-shortener",
		Nodes: []deployer.DeploymentNode{
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "App Server",
				Config: map[string]interface{}{
					"replicas":     1,
					"instanceSize": "medium",
				},
			},
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]interface{}{
					"strategy": "cache-aside",
				},
			},
			{ID: "db-1", Type: "db", Name: "PostgreSQL"},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app-1", TargetID: "cache-1"},
			{SourceID: "app-1", TargetID: "db-1"},
		},
	}
	raw, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status: want 422, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out deployer.ResourceBudgetErrorJSON
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Tier != "explorer" {
		t.Fatalf("tier: got %q want explorer", out.Tier)
	}
	if out.Requested["cpu"] != "750m" {
		t.Fatalf("requested cpu: got %q want 750m", out.Requested["cpu"])
	}
	if len(out.Breakdown) == 0 {
		t.Fatal("expected breakdown entries")
	}
}

func TestHandleCreateDeployment_TierTopologyForbidden(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "ecommerce",
		Archetype: "ecommerce",
		Nodes: []deployer.DeploymentNode{
			{ID: "lb-1", Type: "lb", Name: "Load Balancer"},
			{
				ID:     "app-1",
				Type:   "app-server",
				Name:   "App Server",
				Config: map[string]interface{}{"replicas": 1, "instanceSize": "small"},
			},
			{ID: "cache-1", Type: "cache", Name: "Redis", Config: map[string]interface{}{"strategy": "cache-aside"}},
			{ID: "db-1", Type: "db", Name: "PostgreSQL"},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "lb-1", TargetID: "app-1"},
			{SourceID: "app-1", TargetID: "cache-1"},
			{SourceID: "app-1", TargetID: "db-1"},
		},
	}
	raw, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status: want 403, got %d body=%s", rec.Code, rec.Body.String())
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

func TestHandleCreateDeployment_RequestBodyTooLarge(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	huge := strings.Repeat("a", int(maxJSONBodyBytes))
	tooLarge := fmt.Sprintf(`{"template":"%s","archetype":"x","nodes":[],"connections":[]}`, huge)
	req := httptest.NewRequest(http.MethodPost, "/deployments", strings.NewReader(tooLarge))
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
	if !strings.Contains(er.Error, "request body too large") {
		t.Fatalf("error message: %q", er.Error)
	}
}

func TestHandleCreateDeployment_SimulationRecommended_PartialTopology(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")

	future := time.Now().Add(24 * time.Hour)
	s := newFakeDeploymentStore()
	s.users = map[string]store.UserRecord{
		"dev-user": {IsPremium: true, PremiumExpiresAt: &future},
	}
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// Chained LBs (lb→lb) are valid but unsupported → simulation_recommended.
	// Each LB still needs a direct lb→app-server edge for ValidateRuntimeContract.
	// Multi app-server nodes are rejected by ValidateTopology, so they cannot drive this case.
	body := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "standard",
		Nodes: []deployer.DeploymentNode{
			{ID: "lb1", Type: "lb", Name: "lb1", Config: map[string]interface{}{}},
			{ID: "lb2", Type: "lb", Name: "lb2", Config: map[string]interface{}{}},
			{ID: "app", Type: "app-server", Name: "app", Config: map[string]interface{}{}},
			{ID: "db", Type: "db", Name: "db", Config: map[string]interface{}{}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "lb1", TargetID: "lb2"},
			{SourceID: "lb1", TargetID: "app"},
			{SourceID: "lb2", TargetID: "app"},
			{SourceID: "app", TargetID: "db"},
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
	if out.Assessment.Verdict != deployer.VerdictPartial && out.Assessment.Verdict != deployer.VerdictSimulate {
		t.Fatalf("verdict: got %q want partial or simulate", out.Assessment.Verdict)
	}
	if out.Assessment.Explanation == "" {
		t.Fatal("empty assessment explanation")
	}
}

func TestHandleCreateDeployment_MultipleAppServersRejected(t *testing.T) {
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
			{SourceID: "a", TargetID: "db"},
			{SourceID: "b", TargetID: "db"},
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
	if !strings.Contains(rec.Body.String(), "only one app-server") {
		t.Fatalf("body: want multi app-server error, got %s", rec.Body.String())
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

func TestHandleCreateDeployment_TemplateCapabilityWarnings(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")

	future := time.Now().Add(24 * time.Hour)
	s := newFakeDeploymentStore()
	s.users = map[string]store.UserRecord{
		"dev-user": {IsPremium: true, PremiumExpiresAt: &future},
	}
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "standard",
		Nodes: []deployer.DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app", Config: map[string]interface{}{}},
			{ID: "db", Type: "db", Name: "db", Config: map[string]interface{}{}},
			{ID: "mq", Type: "mq", Name: "mq", Config: map[string]interface{}{}},
			{ID: "worker", Type: "worker", Name: "worker", Config: map[string]interface{}{}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app", TargetID: "db"},
			{SourceID: "app", TargetID: "mq"},
			{SourceID: "worker", TargetID: "mq"},
			{SourceID: "worker", TargetID: "db"},
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
		Assessment struct {
			Verdict  string `json:"verdict"`
			Warnings []struct {
				Code    string   `json:"code"`
				Message string   `json:"message"`
				NodeIDs []string `json:"nodeIds"`
			} `json:"warnings"`
		} `json:"assessment"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Assessment.Verdict != deployer.VerdictFull {
		t.Fatalf("verdict: got %q", out.Assessment.Verdict)
	}
	if len(out.Assessment.Warnings) != 1 {
		t.Fatalf("warnings: want 1, got %d (%+v)", len(out.Assessment.Warnings), out.Assessment.Warnings)
	}
	if out.Assessment.Warnings[0].Code != deployer.WarningCodeTemplateNoAsync {
		t.Fatalf("warning code: got %q", out.Assessment.Warnings[0].Code)
	}
	if out.Assessment.Warnings[0].Message == "" {
		t.Fatal("empty warning message")
	}
	if len(out.Assessment.Warnings[0].NodeIDs) != 2 {
		t.Fatalf("nodeIds: got %v", out.Assessment.Warnings[0].NodeIDs)
	}
}

func TestHandleCreateDeployment_ReadConsistencyClampedWarning(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")

	future := time.Now().Add(24 * time.Hour)
	s := newFakeDeploymentStore()
	s.users = map[string]store.UserRecord{
		"dev-user": {IsPremium: true, PremiumExpiresAt: &future},
	}
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "standard",
		Nodes: []deployer.DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app", Config: map[string]interface{}{}},
			{
				ID:   "db",
				Type: "db",
				Name: "db",
				Config: map[string]interface{}{
					"readReplicas":    1,
					"readConsistency": "monotonic",
					"replicationMode": "async",
				},
			},
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
		Assessment struct {
			Verdict  string `json:"verdict"`
			Warnings []struct {
				Code    string   `json:"code"`
				Message string   `json:"message"`
				NodeIDs []string `json:"nodeIds"`
			} `json:"warnings"`
		} `json:"assessment"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Assessment.Verdict != deployer.VerdictFull {
		t.Fatalf("verdict: got %q", out.Assessment.Verdict)
	}
	var found bool
	for _, w := range out.Assessment.Warnings {
		if w.Code == deployer.WarningCodeReadConsistencyClamped {
			found = true
			if len(w.NodeIDs) != 1 || w.NodeIDs[0] != "db" {
				t.Fatalf("nodeIds: got %v", w.NodeIDs)
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected read_consistency_clamped warning, got %+v", out.Assessment.Warnings)
	}
}

func TestHandleCreateDeployment_ConcurrentLimit_RejectsSecondActive(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "")

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

func TestHandleCreateDeployment_MonthlySessionLimit(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "")

	s := newFakeDeploymentStore()
	used := 5
	s.forcedMonthlyCount = &used
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
	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d body=%s", rec.Code, rec.Body.String())
	}
	var er struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &er); err != nil {
		t.Fatal(err)
	}
	if er.Reason != "monthly_session_limit" {
		t.Fatalf("reason = %q, want monthly_session_limit body=%s", er.Reason, rec.Body.String())
	}
	if er.Error != monthlySessionLimitMessage {
		t.Fatalf("error = %q, want %q", er.Error, monthlySessionLimitMessage)
	}
}

func TestHandleCreateDeployment_RoomBudget_EnqueuesWhenFull(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	// Room is large enough for one url-shortener, but forced occupancy fills it.
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "5000m")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "15000Mi")
	t.Setenv("MAX_DEPLOY_QUEUE", "20")

	s := newFakeDeploymentStore()
	s.forcedOccupancy = &store.AdmissionOccupancy{CPUMillicores: 5000, MemoryMi: 15000}
	s.users = map[string]store.UserRecord{"dev-user": {IsInternal: true}}
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	if h.roomBudget == nil {
		t.Fatal("expected room budget config from env")
	}
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
	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Status               string `json:"status"`
		QueuePosition        int    `json:"queuePosition"`
		EstimatedWaitSeconds int    `json:"estimatedWaitSeconds"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "queued" {
		t.Fatalf("status = %q, want queued body=%s", out.Status, rec.Body.String())
	}
	if out.QueuePosition != 1 {
		t.Fatalf("queuePosition = %d, want 1", out.QueuePosition)
	}
	if out.EstimatedWaitSeconds <= 0 {
		t.Fatalf("estimatedWaitSeconds = %d, want > 0", out.EstimatedWaitSeconds)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.active) != 1 || s.active[0].Status != "queued" {
		t.Fatalf("active = %+v, want one queued", s.active)
	}
}

func TestHandleCreateDeployment_RoomBudget_RejectsWhenExceedsEmptyRoom(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "100m")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "100Mi")

	s := newFakeDeploymentStore()
	s.users = map[string]store.UserRecord{"dev-user": {IsInternal: true}}
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
	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d body=%s", rec.Code, rec.Body.String())
	}
	var er struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &er); err != nil {
		t.Fatal(err)
	}
	if er.Reason != "design_exceeds_cluster_capacity" {
		t.Fatalf("reason = %q, want design_exceeds_cluster_capacity body=%s", er.Reason, rec.Body.String())
	}
	if er.Error != designExceedsClusterMessage {
		t.Fatalf("error = %q, want %q", er.Error, designExceedsClusterMessage)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.active) != 0 || len(s.byNS) != 0 {
		t.Fatalf("want zero rows, got active=%d byNS=%d", len(s.active), len(s.byNS))
	}
}

func TestHandleCreateDeployment_RoomBudget_QueueFull(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "5000m")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "15000Mi")
	t.Setenv("MAX_DEPLOY_QUEUE", "1")

	s := newFakeDeploymentStore()
	s.forcedOccupancy = &store.AdmissionOccupancy{CPUMillicores: 5000, MemoryMi: 15000}
	s.users = map[string]store.UserRecord{"dev-user": {IsInternal: true}}
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

	// Free the per-user slot so we hit queue_full, not user capacity.
	// Keep one queued row owned by another user to fill MAX_DEPLOY_QUEUE=1.
	s.mu.Lock()
	for _, a := range s.active {
		row := s.byNS[a.Namespace]
		row.Status = "terminated"
		s.byNS[a.Namespace] = row
	}
	s.active = []store.ActiveDeploymentRow{{
		ID: "other", Namespace: "other-ns", Template: "url-shortener", Status: "queued",
		CreatedAt: time.Now().Add(-time.Minute), ExpiresAt: time.Now().Add(time.Hour),
	}}
	s.byNS["other-ns"] = store.DeploymentRow{
		ID: "other", UserID: "other-user", Namespace: "other-ns", Status: "queued",
		CreatedAt: time.Now().Add(-time.Minute), ExpiresAt: time.Now().Add(time.Hour),
	}
	s.admittedByNS["other-ns"] = store.AdmissionOccupancy{CPUMillicores: 900, MemoryMi: 2000}
	s.mu.Unlock()

	req2 := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second create: want 429, got %d body=%s", rec2.Code, rec2.Body.String())
	}
	var er struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(rec2.Body.Bytes(), &er)
	if er.Reason != "queue_full" {
		t.Fatalf("reason = %q, want queue_full body=%s", er.Reason, rec2.Body.String())
	}
}

func TestHandleCreateDeployment_RoomBudget_AdmitsWhenFits(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "100000m")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "200000Mi")

	s := newFakeDeploymentStore()
	s.users = map[string]store.UserRecord{"dev-user": {IsInternal: true}}
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
	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d body=%s", rec.Code, rec.Body.String())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.active) != 1 {
		t.Fatalf("active = %d, want 1", len(s.active))
	}
	w := s.admittedByNS[s.active[0].Namespace]
	if w.CPUMillicores <= 0 || w.MemoryMi <= 0 {
		t.Fatalf("expected persisted admitted weights, got %+v", w)
	}
}

func TestHandleCreateDeployment_RoomBudget_UnsetSkipsGate(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "")

	s := newFakeDeploymentStore()
	full := store.AdmissionOccupancy{CPUMillicores: 1e9, MemoryMi: 1e9}
	s.forcedOccupancy = &full
	s.users = map[string]store.UserRecord{"dev-user": {IsInternal: true}}
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	if h.roomBudget != nil {
		t.Fatal("expected room gate off when budget env unset")
	}
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
	req := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("want 201 with gate off, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleCreateDeployment_ConcurrentLimit_InternalTier(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "")

	s := newFakeDeploymentStore()
	s.users = map[string]store.UserRecord{
		"dev-user": {IsInternal: true},
	}
	limit := store.GetTierLimits(store.TierInternal).MaxConcurrent
	for i := 0; i < limit-1; i++ {
		namespace := fmt.Sprintf("existing-internal-%d", i)
		s.byNS[namespace] = store.DeploymentRow{
			ID:        fmt.Sprintf("existing-%d", i),
			UserID:    "dev-user",
			Namespace: namespace,
			Status:    "running",
		}
		s.active = append(s.active, store.ActiveDeploymentRow{
			ID:        fmt.Sprintf("existing-%d", i),
			Namespace: namespace,
			Status:    "running",
		})
	}

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

	atLimitReq := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	atLimitReq.Header.Set("Content-Type", "application/json")
	atLimitRec := httptest.NewRecorder()
	mux.ServeHTTP(atLimitRec, atLimitReq)
	if atLimitRec.Code != http.StatusCreated {
		t.Fatalf("%dth create: want 201, got %d body=%s", limit, atLimitRec.Code, atLimitRec.Body.String())
	}

	overLimitReq := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	overLimitReq.Header.Set("Content-Type", "application/json")
	overLimitRec := httptest.NewRecorder()
	mux.ServeHTTP(overLimitRec, overLimitReq)
	if overLimitRec.Code != http.StatusTooManyRequests {
		t.Fatalf("%dth create: want 429, got %d body=%s", limit+1, overLimitRec.Code, overLimitRec.Body.String())
	}
}

func TestHandleCreateDeployment_PremiumTemplateDeniedForFree(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")

	restore := templates.ReplaceLoadedTemplatesForTest(append(templates.Registry(), templates.TemplateSpec{
		ID:          "premium-only-test",
		Name:        "Premium Only Test",
		Available:   true,
		Premium:     true,
		Deployable:  true,
		Simulatable: true,
	}))
	defer restore()

	s := newFakeDeploymentStore()
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := deployer.DeploymentDesign{
		Template:  "premium-only-test",
		Archetype: "premium-only-test",
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

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status: want 403, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleCreateDeployment_EngineerSessionMinutesStored(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")

	future := time.Now().Add(24 * time.Hour)
	s := newFakeDeploymentStore()
	s.users = map[string]store.UserRecord{
		"dev-user": {IsPremium: true, PremiumExpiresAt: &future},
	}
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	session := 12
	body := deployer.DeploymentDesign{
		Template:       "url-shortener",
		Archetype:      "standard",
		SessionMinutes: &session,
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

	var created struct {
		Namespace string `json:"namespace"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	row, err := s.GetDeployment(context.Background(), created.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Now().Add(12 * time.Minute)
	if row.ExpiresAt.Before(want.Add(-2*time.Minute)) || row.ExpiresAt.After(want.Add(2*time.Minute)) {
		t.Fatalf("expiresAt: got %v want ~%v", row.ExpiresAt, want)
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
	bus := metrics.NewEventBus()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, bus)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"template":"url-shortener","rps":5,"durationSeconds":30}`
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
	events := bus.Drain("ns-ok")
	if len(events) != 1 {
		t.Fatalf("events: %+v", events)
	}
	if events[0].Kind != "loadgen_start" || events[0].Source != "loadgen" {
		t.Fatalf("event: %+v", events[0])
	}
	if events[0].Detail["template"] != "url-shortener" || events[0].Detail["rps"] != "5" {
		t.Fatalf("detail: %+v", events[0].Detail)
	}
	_ = lg.Stop(context.Background(), "ns-ok")
}

func TestHandleStartLoadGen_unknownSchedule(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"template":"social-feed","rps":10,"scheduleId":"missing-schedule"}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns-bad/loadgen", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleGetTemplateLoadgen_socialFeed(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/templates/social-feed/loadgen", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var view templates.LoadgenConfigView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, s := range view.Schedules {
		ids[s.ID] = true
	}
	if !ids["steady"] || !ids["world_cup_goal"] {
		t.Fatalf("schedules: %+v", view.Schedules)
	}
	found := false
	for _, p := range view.Params {
		if p.Key == "celebrityPostPct" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("params missing celebrityPostPct: %+v", view.Params)
	}
	if len(view.Endpoints) != 3 {
		t.Fatalf("endpoints: want 3, got %+v", view.Endpoints)
	}
	weights := map[string]int{}
	for _, ep := range view.Endpoints {
		weights[ep.Method+" "+ep.PathTemplate] = ep.Weight
	}
	if weights["GET /feed/{userId}"] != 60 || weights["POST /post"] != 30 || weights["GET /users/{userId}"] != 10 {
		t.Fatalf("endpoint weights: %+v", weights)
	}
}

func TestHandleGetTemplateLoadgen_unknown(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/templates/no-such-template/loadgen", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: want 404, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleUpdateLoadGen_notRunning(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"rps":20}`
	req := httptest.NewRequest(http.MethodPatch, "/deployments/ns-idle/loadgen", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status: want 409, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleUpdateLoadGen_running(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	bus := metrics.NewEventBus()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, bus)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	start := `{"template":"url-shortener","rps":5,"durationSeconds":30}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns-rate/loadgen", strings.NewReader(start))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("start status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	patch := `{"rps":20}`
	req = httptest.NewRequest(http.MethodPatch, "/deployments/ns-rate/loadgen", strings.NewReader(patch))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	events := bus.Drain("ns-rate")
	found := false
	for _, e := range events {
		if e.Kind == "loadgen_rate" && e.Detail["rps"] == "20" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing loadgen_rate event: %+v", events)
	}
	_ = lg.Stop(context.Background(), "ns-rate")
}

func TestHandleUpdateLoadGen_mixRunning(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	bus := metrics.NewEventBus()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, bus)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	start := `{"template":"url-shortener","rps":5,"durationSeconds":30}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns-mix/loadgen", strings.NewReader(start))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("start status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	patch := `{"distribution":{"GET /r/{code}":50,"POST /shorten":50}}`
	req = httptest.NewRequest(http.MethodPatch, "/deployments/ns-mix/loadgen", strings.NewReader(patch))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	events := bus.Drain("ns-mix")
	found := false
	for _, e := range events {
		if e.Kind == "loadgen_mix" && e.Detail["GET /r/{code}"] == "50" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing loadgen_mix event: %+v", events)
	}
	_ = lg.Stop(context.Background(), "ns-mix")
}

func TestHandleUpdateLoadGen_emptyBody(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodPatch, "/deployments/ns-x/loadgen", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleUpdateLoadGen_badRPS(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodPatch, "/deployments/ns-x/loadgen", strings.NewReader(`{"rps":0}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleStartLoadGen_RequestBodyTooLarge(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, lg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	huge := strings.Repeat("a", int(maxJSONBodyBytes))
	tooLarge := fmt.Sprintf(`{"template":"url-shortener","rps":5,"baseUrl":"http://%s"}`, huge)
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns-big/loadgen", strings.NewReader(tooLarge))
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
	if !strings.Contains(er.Error, "request body too large") {
		t.Fatalf("error message: %q", er.Error)
	}
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

func TestDeleteDuringProvision_StaysTerminated(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")

	s := newFakeDeploymentStore()
	deployers := map[string]deployer.ComponentDeployer{
		"db": &fakeBlockingDeployer{},
		"app-server": &fakeBlockingDeployer{
			deployFn: func(ctx context.Context, _ deployer.DeployConfig) error {
				<-ctx.Done()
				return ctx.Err()
			},
		},
	}
	h := NewHandler(deployers, s, nil, nil, nil, nil, nil, nil, nil)
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

	createReq := httptest.NewRequest(http.MethodPost, "/deployments", bytes.NewReader(raw))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status: want 201, got %d body=%s", createRec.Code, createRec.Body.String())
	}
	var created struct {
		Namespace string `json:"namespace"`
	}
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.Namespace == "" {
		t.Fatal("missing namespace in create response")
	}

	deadline := time.Now().Add(4 * time.Second)
	registered := false
	for time.Now().Before(deadline) {
		h.provisionMu.Lock()
		_, ok := h.provisionCancel[created.Namespace]
		h.provisionMu.Unlock()
		if ok {
			registered = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !registered {
		t.Fatal("expected provisioning cancel func to be registered")
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/deployments/"+created.Namespace, nil)
	deleteRec := httptest.NewRecorder()
	mux.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete status: want 204, got %d body=%s", deleteRec.Code, deleteRec.Body.String())
	}

	waitDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(waitDeadline) {
		row, err := s.GetDeployment(context.Background(), created.Namespace)
		if err != nil {
			t.Fatalf("get deployment: %v", err)
		}
		if row.Status == "terminated" {
			time.Sleep(150 * time.Millisecond)
			row2, err := s.GetDeployment(context.Background(), created.Namespace)
			if err != nil {
				t.Fatalf("get deployment (stability): %v", err)
			}
			if row2.Status != "terminated" {
				t.Fatalf("status changed after termination: %q", row2.Status)
			}
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("expected deployment status to become terminated")
}

type fakeDeploymentStore struct {
	mu                 sync.Mutex
	byNS               map[string]store.DeploymentRow
	active             []store.ActiveDeploymentRow
	history            []store.DeploymentHistoryRow
	historyErr         error
	historyUser        string
	historyLimit       int
	nextSeq            int
	users              map[string]store.UserRecord
	defaultUser        store.UserRecord
	admittedByNS       map[string]store.AdmissionOccupancy
	forcedOccupancy    *store.AdmissionOccupancy
	sumOccErr          error
	forcedMonthlyCount *int
	monthlyCountErr    error
}

func newFakeDeploymentStore() *fakeDeploymentStore {
	return &fakeDeploymentStore{
		byNS:         make(map[string]store.DeploymentRow),
		active:       make([]store.ActiveDeploymentRow, 0),
		admittedByNS: make(map[string]store.AdmissionOccupancy),
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

func (f *fakeDeploymentStore) ListUserDeploymentHistory(_ context.Context, userID string, limit int) ([]store.DeploymentHistoryRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.historyUser = userID
	f.historyLimit = limit
	if f.historyErr != nil {
		return nil, f.historyErr
	}
	out := make([]store.DeploymentHistoryRow, len(f.history))
	copy(out, f.history)
	return out, nil
}

func (f *fakeDeploymentStore) CreateDeployment(_ context.Context, params store.CreateDeploymentParams) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSeq++
	id := fmt.Sprintf("dep-%d", f.nextSeq)
	status := params.Status
	if status == "" {
		status = "provisioning"
	}
	row := store.DeploymentRow{
		ID:        id,
		UserID:    params.UserID,
		Namespace: params.Namespace,
		Template:  params.Template,
		Archetype: params.Archetype,
		Status:    status,
		Design:    params.Design,
		ExpiresAt: params.ExpiresAt,
		CreatedAt: time.Now(),
	}
	f.byNS[params.Namespace] = row
	f.admittedByNS[params.Namespace] = store.AdmissionOccupancy{
		CPUMillicores: params.AdmittedCPUm,
		MemoryMi:      params.AdmittedMemMi,
	}
	if status == "running" || status == "provisioning" || status == "queued" {
		f.active = append(f.active, store.ActiveDeploymentRow{
			ID:        id,
			Namespace: params.Namespace,
			Template:  params.Template,
			Status:    status,
			ExpiresAt: params.ExpiresAt,
			CreatedAt: row.CreatedAt,
		})
	}
	return id, nil
}

func (f *fakeDeploymentStore) ListQueuedDeployments(_ context.Context) ([]store.QueuedDeploymentRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.QueuedDeploymentRow, 0)
	for _, r := range f.active {
		if r.Status != "queued" {
			continue
		}
		row := f.byNS[r.Namespace]
		w := f.admittedByNS[r.Namespace]
		cpu := w.CPUMillicores
		mem := w.MemoryMi
		out = append(out, store.QueuedDeploymentRow{
			ID:            row.ID,
			UserID:        row.UserID,
			Namespace:     r.Namespace,
			Template:      r.Template,
			Status:        r.Status,
			ExpiresAt:     r.ExpiresAt,
			CreatedAt:     r.CreatedAt,
			AdmittedCPUm:  &cpu,
			AdmittedMemMi: &mem,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (f *fakeDeploymentStore) CountQueuedDeployments(ctx context.Context) (int, error) {
	rows, err := f.ListQueuedDeployments(ctx)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func (f *fakeDeploymentStore) CountUserDeploymentsSince(_ context.Context, userID string, since time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.monthlyCountErr != nil {
		return 0, f.monthlyCountErr
	}
	if f.forcedMonthlyCount != nil {
		return *f.forcedMonthlyCount, nil
	}
	n := 0
	for _, row := range f.byNS {
		if row.UserID != userID {
			continue
		}
		if row.CreatedAt.Before(since) {
			continue
		}
		if row.Status == "running" {
			n++
			continue
		}
		if row.MetricsSummary != nil {
			if raw, ok := row.MetricsSummary["running_at"]; ok && raw != nil && raw != "" {
				n++
			}
		}
	}
	return n, nil
}

func (f *fakeDeploymentStore) ClaimQueued(_ context.Context, namespace string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.byNS[namespace]
	if !ok || row.Status != "queued" {
		return false, nil
	}
	row.Status = "provisioning"
	f.byNS[namespace] = row
	for i := range f.active {
		if f.active[i].Namespace == namespace {
			f.active[i].Status = "provisioning"
		}
	}
	return true, nil
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
			if status == "running" || status == "provisioning" || status == "queued" {
				r.Status = status
				active = append(active, r)
			}
			continue
		}
		active = append(active, r)
	}
	f.active = active
	if status != "running" && status != "provisioning" {
		// Queued still holds weight columns but does not occupy the room;
		// only drop admitted map when leaving room-relevant statuses.
		if status != "queued" {
			delete(f.admittedByNS, namespace)
		}
	}
	return nil
}

func (f *fakeDeploymentStore) SumActiveAdmissionWeights(_ context.Context) (store.AdmissionOccupancy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sumOccErr != nil {
		return store.AdmissionOccupancy{}, f.sumOccErr
	}
	if f.forcedOccupancy != nil {
		return *f.forcedOccupancy, nil
	}
	var occ store.AdmissionOccupancy
	for _, r := range f.active {
		if r.Status != "running" && r.Status != "provisioning" {
			continue
		}
		w := f.admittedByNS[r.Namespace]
		occ.CPUMillicores += w.CPUMillicores
		occ.MemoryMi += w.MemoryMi
	}
	return occ, nil
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

func (f *fakeDeploymentStore) GetUser(_ context.Context, userID string) (store.UserRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if userID == "" {
		return store.UserRecord{}, store.ErrUserNotFound
	}
	if f.users != nil {
		if u, ok := f.users[userID]; ok {
			return u, nil
		}
	}
	return f.defaultUser, nil
}

func TestDesignHasLoadBalancer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   map[string]any
		want bool
	}{
		{name: "canonical lb", in: map[string]any{"nodes": []any{map[string]any{"type": "lb"}}}, want: true},
		{name: "legacy load_balancer", in: map[string]any{"nodes": []any{map[string]any{"type": "load_balancer"}}}, want: true},
		{name: "no lb", in: map[string]any{"nodes": []any{map[string]any{"type": "app-server"}}}, want: false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := designHasLoadBalancer(tc.in); got != tc.want {
				t.Fatalf("designHasLoadBalancer() = %v, want %v", got, tc.want)
			}
		})
	}
}
