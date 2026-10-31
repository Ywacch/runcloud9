package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/designplan"
	"github.com/ywacch/cloud9/pkg/templates"
)

func TestHandleDesignPlan_ok(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("url-shortener")
	if tmpl == nil || tmpl.Preset == nil {
		t.Fatal("template preset missing")
	}

	design := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Version:   "1.0",
		Archetype: "standard",
	}
	for _, n := range tmpl.Preset.Nodes {
		cfg := map[string]interface{}{}
		for k, v := range n.Config {
			cfg[k] = v
		}
		design.Nodes = append(design.Nodes, deployer.DeploymentNode{
			ID: n.ID, Type: n.Type, Name: n.Label, Config: cfg,
		})
	}
	for _, e := range tmpl.Preset.Edges {
		design.Connections = append(design.Connections, deployer.DeploymentConn{
			SourceID: e.SourceID, TargetID: e.TargetID, Protocol: e.Protocol,
		})
	}
	deployer.NormalizeDesign(&design)

	body, err := json.Marshal(map[string]any{"design": design})
	if err != nil {
		t.Fatal(err)
	}

	s := newFakeDeploymentStore()
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/designs/plan", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d body=%s", rec.Code, rec.Body.String())
	}
	var plan designplan.Plan
	if err := json.NewDecoder(rec.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if !plan.Budget.RoomFits {
		t.Fatalf("budget: %+v", plan.Budget)
	}
}

func TestHandleDesignPlan_unknownField(t *testing.T) {
	t.Parallel()
	body := []byte(`{"design":{"template":"url-shortener","nodes":[],"connections":[]},"extra":true}`)
	s := newFakeDeploymentStore()
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/designs/plan", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: %d body=%s", rec.Code, rec.Body.String())
	}
}
