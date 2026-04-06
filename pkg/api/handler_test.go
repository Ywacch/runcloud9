package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestHandleGetTemplates(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil)
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
	if len(list) != 3 {
		t.Fatalf("want 3 templates, got %d", len(list))
	}
	if list[0].ID != "url-shortener" || !list[0].Available {
		t.Fatalf("first template: %+v", list[0])
	}
	if list[1].ID != "ecommerce" || list[1].Available {
		t.Fatalf("ecommerce should be unavailable: %+v", list[1])
	}
}

func TestHandleCreateDeployment_UnavailableRegistryTemplate(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil)
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

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &er)
	if er.Error == "" || !strings.Contains(er.Error, "not yet available") {
		t.Fatalf("error message: %q", er.Error)
	}
}
