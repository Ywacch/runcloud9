package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/loadgen"
)

func TestHandleGetTemplates(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil)
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
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil)
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
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil)
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
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil)
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

func TestHandleStartLoadGen_RequiresTemplate(t *testing.T) {
	t.Parallel()
	lg := loadgen.NewLoadGenerator()
	h := NewHandler(nil, nil, nil, nil, nil, nil, lg, nil)
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
	h := NewHandler(nil, nil, nil, nil, nil, nil, lg, nil)
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
	h := NewHandler(nil, nil, nil, nil, nil, nil, lg, nil)
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
