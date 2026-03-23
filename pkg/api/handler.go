package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ywacch/ganymede/pkg/deployer"
	"github.com/ywacch/ganymede/pkg/store"
	"github.com/ywacch/ganymede/pkg/seeder"
	"github.com/ywacch/ganymede/pkg/teardown"
)

// Handler holds dependencies for the HTTP layer.
// Rather than using global variables, we pass dependencies in via the struct.
// This makes testing easier as you can swap in a mock deployer without
// touching global state.
type Handler struct {
	deployers map[string]deployer.ComponentDeployer
	store     *store.DeploymentStore
	scheduler *teardown.Scheduler
	seeder    *seeder.Seeder
}

func NewHandler(
	deployers map[string]deployer.ComponentDeployer,
	store *store.DeploymentStore,
	scheduler *teardown.Scheduler,
	seed *seeder.Seeder,
) *Handler {
	return &Handler{deployers: deployers, store: store, scheduler: scheduler, seeder: seed}
}

// RegisterRoutes attaches handlers to the mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /deployments", h.handleCreateDeployment)
	mux.HandleFunc("DELETE /deployments/{id}", h.handleDeleteDeployment)
	mux.HandleFunc("GET /health", h.handleHealth)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func (h *Handler) handleCreateDeployment(w http.ResponseWriter, r *http.Request) {
	// context.WithTimeout creates a child context that auto-cancels
	// after `timeout` minutes. This context us passed to every deployer call so that if
	// the whole operation takes too long, all in-flight work gets cancelled.
	// Always defer cancel() immediately after WithTimeout because if you forget,
	// the context leaks until the parent is cancelled.
	timeout := 2 * time.Minute
	if os.Getenv("DEPLOYER") == "k8s" {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
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

	namespace := fmt.Sprintf("ganymede-%s-%d", design.Template, time.Now().Unix())
	expiresAt := time.Now().Add(10 * time.Minute)

	if h.store != nil {
		var designMap map[string]any
		if b, err := json.Marshal(design); err != nil {
			writeError(w, http.StatusInternalServerError, "invalid design")
			return
		} else if err := json.Unmarshal(b, &designMap); err != nil {
			writeError(w, http.StatusInternalServerError, "invalid design")
			return
		}
		_, err := h.store.CreateDeployment(ctx, store.CreateDeploymentParams{
			Namespace: namespace,
			Template:  design.Template,
			Archetype: design.Archetype,
			Design:    designMap,
			ExpiresAt: expiresAt,
			UserID:    getUserID(r),
		})
		if err != nil {
			log.Printf("create deployment record: %v", err)
			writeError(w, http.StatusInternalServerError, "failed to create deployment record")
			return
		}
	}

	result, err := h.orchestrate(ctx, design, namespace)
	if err != nil {
		log.Printf("orchestration failed for namespace=%s: %v", namespace, err)
		if h.store != nil {
			_ = h.store.UpdateStatus(context.Background(), namespace, "failed")
		}
		writeError(w, http.StatusInternalServerError, "deployment failed")
		return
	}

	if h.store != nil {
		_ = h.store.UpdateStatus(context.Background(), namespace, "running")
	}
	if h.scheduler != nil {
		h.scheduler.Schedule(namespace, expiresAt)
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

	if h.seeder != nil {
		if err := h.seeder.SeedDatabase(ctx, namespace, design.Template); err != nil {
			log.Printf("seeding failed for namespace=%s (continuing deployment): %v", namespace, err)
		}
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
	if err := deployer.ValidateTopology(d); err != nil {
		return err
	}
	return nil
}

func (h *Handler) handleDeleteDeployment(w http.ResponseWriter, r *http.Request) {
	//PathValue is Go 1.22+:extracts {id} from the URL pattern.
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	if h.scheduler != nil {
		h.scheduler.Cancel(namespace)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	d := h.deployers["db"]
	if err := d.Teardown(ctx, namespace); err != nil {
		log.Printf("teardown %s: %v", namespace, err)
		writeError(w, http.StatusInternalServerError, "teardown failed")
		return
	}
	if h.store != nil {
		_ = h.store.UpdateStatus(context.Background(), namespace, "terminated")
	}
	w.WriteHeader(http.StatusNoContent)
}

// getUserID extracts the sub claim from the Bearer JWT in Authorization header.
// Returns empty string on any failure — unauthenticated requests are allowed.
func getUserID(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}
	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	if token == "" {
		return ""
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	return claims.Sub
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
