package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ywacch/ganymede/pkg/deployer"
)

// Handler holds dependencies for the HTTP layer.
// Rather than using global variables, we pass dependencies in via the struct.
// This makes testing easier as you can swap in a mock deployer without
// touching global state.
type Handler struct {
	// deployers maps component type (e.g. "db", "cache") to its deployer.
	// In production this will be K8s implementations. For now, LocalDeployer.
	deployers map[string]deployer.ComponentDeployer
}

func NewHandler(deployers map[string]deployer.ComponentDeployer) *Handler {
	return &Handler{deployers: deployers}
}

// RegisterRoutes attaches handlers to the mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /deployments", h.handleCreateDeployment)
	mux.HandleFunc("GET /health", h.handleHealth)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func (h *Handler) handleCreateDeployment(w http.ResponseWriter, r *http.Request) {
	// context.WithTimeout creates a child context that auto-cancels
	// after 2 minutes. This context us passed to every deployer call so that if
	// the whole operation takes too long, all in-flight work gets cancelled.
	// Always defer cancel() immediately after WithTimeout because if you forget,
	// the context leaks until the parent is cancelled.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	var design deployer.DeploymentDesign
	if err := json.NewDecoder(r.Body).Decode(&design); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if err := validateDesign(design); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Generate a namespace for this deployment.
	// Format: ganymede-{template}-{timestamp}
	// UNDERSTAND: Each deployment gets its own K8s namespace. This is how we
	// isolate deployments from each other. Deleting the namespace deletes
	// everything inside it: pods, services, config maps. Clean teardown.
	namespace := fmt.Sprintf("ganymede-%s-%d", design.Template, time.Now().Unix())

	result, err := h.orchestrate(ctx, design, namespace)
	if err != nil {
		log.Printf("orchestration failed for namespace=%s: %v", namespace, err)
		writeError(w, http.StatusInternalServerError, "deployment failed")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(result)
}

// orchestrate deploys all nodes in the design sequentially.
// On any failure, it attempts to teardown already-deployed nodes (rollback).
func (h *Handler) orchestrate(
	ctx context.Context,
	design deployer.DeploymentDesign,
	namespace string,
) (*deployer.DeploymentResult, error) {

	deployed := []deployer.DeploymentNode{}

	for _, node := range design.Nodes {
		d, ok := h.deployers[node.Type]
		if !ok {
			// No deployer registered for this type. use a generic local one for now
			d = &deployer.LocalDeployer{NodeType: node.Type}
		}

		cfg := deployer.DeployConfig{
			Namespace: namespace,
			Node:      node,
			Design:    design,
		}

		if err := d.Deploy(ctx, cfg); err != nil {
			// Rollback on partial failure. We iterate the already-
			// deployed nodes and teardown each one. We use context.Background()
			// here (not the original ctx) because the original ctx may already
			// be cancelled — we want teardown to proceed regardless.
			log.Printf("deploy failed for node=%s, rolling back %d nodes", node.Name, len(deployed))
			h.rollback(context.Background(), deployed, namespace)
			return nil, fmt.Errorf("deploy node %s: %w", node.Name, err)
		}

		deployed = append(deployed, node)
	}

	return &deployer.DeploymentResult{
		DeploymentID: namespace,
		Namespace:    namespace,
		Status:       "running",
	}, nil
}

// rollback tears down all successfully deployed nodes after a partial failure.
func (h *Handler) rollback(ctx context.Context, nodes []deployer.DeploymentNode, namespace string) {
	for _, node := range nodes {
		d, ok := h.deployers[node.Type]
		if !ok {
			d = &deployer.LocalDeployer{NodeType: node.Type}
		}
		if err := d.Teardown(ctx, namespace); err != nil {
			// Log but don't fail — best-effort rollback
			log.Printf("rollback teardown failed for node=%s: %v", node.Name, err)
		}
	}
}

func validateDesign(d deployer.DeploymentDesign) error {
	if d.Template == "" {
		return fmt.Errorf("template is required")
	}
	if d.Archetype == "" {
		return fmt.Errorf("archetype is required")
	}
	if len(d.Nodes) == 0 {
		return fmt.Errorf("design must contain at least one node")
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
