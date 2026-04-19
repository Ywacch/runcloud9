package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/logs"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/seeder"
	"github.com/ywacch/cloud9/pkg/store"
	"github.com/ywacch/cloud9/pkg/teardown"
	"github.com/ywacch/cloud9/pkg/templates"
	"k8s.io/client-go/kubernetes"
)

// Handler holds dependencies for the HTTP layer.
// Rather than using global variables, we pass dependencies in via the struct.
// This makes testing easier as you can swap in a mock deployer without
// touching global state.
type Handler struct {
	deployers        map[string]deployer.ComponentDeployer
	store            *store.DeploymentStore
	scheduler        *teardown.Scheduler
	seeder           *seeder.Seeder
	metricsCollector *metrics.MetricsCollector
	clientset        kubernetes.Interface
	loadgen          *loadgen.LoadGenerator
	eventBus         *metrics.EventBus
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// UNDERSTAND: in production, check origin against allowed list.
		// For development, allow all origins.
		return true
	},
}

func NewHandler(
	deployers map[string]deployer.ComponentDeployer,
	store *store.DeploymentStore,
	scheduler *teardown.Scheduler,
	seed *seeder.Seeder,
	metricsCollector *metrics.MetricsCollector,
	clientset kubernetes.Interface,
	lg *loadgen.LoadGenerator,
	eventBus *metrics.EventBus,
) *Handler {
	return &Handler{
		deployers:        deployers,
		store:            store,
		scheduler:        scheduler,
		seeder:           seed,
		metricsCollector: metricsCollector,
		clientset:        clientset,
		loadgen:          lg,
		eventBus:         eventBus,
	}
}

// RegisterRoutes attaches handlers to the mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /deployments", h.handleCreateDeployment)
	mux.HandleFunc("GET /deployments", h.handleListDeployments)
	mux.HandleFunc("GET /deployments/{id}", h.handleGetDeployment)
	mux.HandleFunc("DELETE /deployments/{id}", h.handleDeleteDeployment)
	mux.HandleFunc("GET /deployments/{id}/metrics", h.handleMetricsStream)
	mux.HandleFunc("GET /deployments/{id}/logs", h.handleLogsStream)
	mux.HandleFunc("GET /deployments/{id}/proxy/{component}", h.handleProxy)
	mux.HandleFunc("GET /deployments/{id}/proxy/{component}/{path...}", h.handleProxy)
	mux.HandleFunc("POST /deployments/{id}/proxy/{component}", h.handleProxy)
	mux.HandleFunc("POST /deployments/{id}/proxy/{component}/{path...}", h.handleProxy)
	mux.HandleFunc("OPTIONS /deployments/{id}/proxy/{component}", h.handleProxy)
	mux.HandleFunc("OPTIONS /deployments/{id}/proxy/{component}/{path...}", h.handleProxy)
	mux.HandleFunc("POST /deployments/{id}/loadgen", h.handleStartLoadGen)
	mux.HandleFunc("DELETE /deployments/{id}/loadgen", h.handleStopLoadGen)
	mux.HandleFunc("GET /health", h.handleHealth)
	mux.HandleFunc("GET /templates", h.handleGetTemplates)
}

// handleMetricsStream upgrades the HTTP connection to WebSocket and streams
// deployment metrics until the client disconnects or the deployment expires.
func (h *Handler) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}

	ctx := r.Context()
	template := ""
	if h.store != nil {
		if row, err := h.store.GetDeployment(ctx, namespace); err == nil {
			template = row.Template
		}
	}
	metrics.StreamMetrics(ctx, conn, h.metricsCollector, namespace, template)
}

func (h *Handler) handleGetDeployment(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "deployment store unavailable")
		return
	}
	row, err := h.store.GetDeployment(r.Context(), namespace)
	if err != nil {
		writeError(w, http.StatusNotFound, "deployment not found")
		return
	}

	resp := map[string]any{
		"id":             row.ID,
		"namespace":      row.Namespace,
		"template":       row.Template,
		"archetype":      row.Archetype,
		"status":         row.Status,
		"expiresAt":      row.ExpiresAt.UTC().Format(time.RFC3339),
		"createdAt":      row.CreatedAt.UTC().Format(time.RFC3339),
		"metricsSummary": row.MetricsSummary,
		"design":         row.Design,
	}
	if row.MetricsSummary != nil {
		if seeded, ok := row.MetricsSummary["seeded"].(bool); ok {
			resp["seeded"] = seeded
		}
	}

	var parsedDesign deployer.DeploymentDesign
	if row.Design != nil {
		if b, err := json.Marshal(row.Design); err == nil {
			_ = json.Unmarshal(b, &parsedDesign)
		}
	}
	if len(parsedDesign.Nodes) > 0 {
		byID := make(map[string]string)
		for _, n := range parsedDesign.Nodes {
			if n.Type != "app-server" {
				continue
			}
			if s := deployer.DisplayCacheStrategyForApp(n.ID, parsedDesign); s != "" {
				byID[n.ID] = s
			}
		}
		if len(byID) > 0 {
			resp["cacheStrategyByAppId"] = byID
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleListDeployments(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "deployment store unavailable")
		return
	}

	// If auth is present, return only the caller's deployments; otherwise return all active deployments.
	// Todo: signed out users shouldnt be able to access or run deployments
	userID := getUserID(r)

	rows, err := h.store.ListActiveDeployments(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Keep response shape stable for the frontend rejoin logic.
	resp := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, map[string]any{
			"id":        row.ID,
			"namespace": row.Namespace,
			"template":  row.Template,
			"status":    row.Status,
			"expiresAt": row.ExpiresAt.UTC().Format(time.RFC3339),
			"createdAt": row.CreatedAt.UTC().Format(time.RFC3339),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleLogsStream upgrades the HTTP connection to WebSocket and streams
// pod logs for all components in the deployment namespace.
func (h *Handler) handleLogsStream(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}

	// UNDERSTAND: if clientset is nil (local dev without K8s),
	// return 503 rather than panic. Log streaming requires a real cluster.
	if h.clientset == nil {
		writeError(w, http.StatusServiceUnavailable, "log streaming requires K8s deployer")
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("log ws upgrade failed: %v", err)
		return
	}

	ctx := r.Context()
	logs.StreamLogs(ctx, conn, h.clientset, namespace)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func (h *Handler) handleGetTemplates(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(templates.Registry())
}

func (h *Handler) handleCreateDeployment(w http.ResponseWriter, r *http.Request) {
	var design deployer.DeploymentDesign
	if err := json.NewDecoder(r.Body).Decode(&design); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if err := validateDesign(design); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	tmpl := templates.GetTemplate(design.Template)
	if tmpl == nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("template %q is not registered — select a valid template", design.Template))
		return
	}
	if !tmpl.Available {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("template %q is not yet available", design.Template))
		return
	}
	if !tmpl.Deployable {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("template %q is not yet deployable — it can only be used in simulation mode", design.Template))
		return
	}
	if tmpl.Premium {
		userID := getUserID(r)
		if userID == "" {
			writeError(w, http.StatusUnauthorized, "premium templates require authentication")
			return
		}
		// TODO: check user's subscription tier from Supabase profiles table
		writeError(w, http.StatusForbidden, fmt.Sprintf(
			"template %q is a premium template — upgrade to Cloud9 Premium to deploy it",
			design.Template,
		))
		return
	}

	assessment := deployer.AssessTopology(design)
	if assessment.Verdict == deployer.VerdictInvalid {
		writeError(w, http.StatusBadRequest, assessment.Explanation)
		return
	}
	if assessment.Verdict != deployer.VerdictFull {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"deploymentId": "",
			"namespace":    "",
			"status":       "simulation_recommended",
			"assessment": map[string]any{
				"verdict":     assessment.Verdict,
				"explanation": assessment.Explanation,
			},
		})
		return
	}

	namespace := fmt.Sprintf("cloud9-%s-%d", design.Template, time.Now().Unix())
	expiresAt := time.Now().Add(9 * time.Minute)

	if h.store != nil {
		var designMap map[string]any
		if b, err := json.Marshal(design); err != nil {
			writeError(w, http.StatusInternalServerError, "invalid design")
			return
		} else if err := json.Unmarshal(b, &designMap); err != nil {
			writeError(w, http.StatusInternalServerError, "invalid design")
			return
		}
		_, err := h.store.CreateDeployment(r.Context(), store.CreateDeploymentParams{
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"deploymentId": namespace,
		"namespace":    namespace,
		"status":       "provisioning",
		"assessment": map[string]any{
			"verdict":     assessment.Verdict,
			"explanation": assessment.Explanation,
		},
	})

	// Continue orchestration in the background after returning the response.
	go func(design deployer.DeploymentDesign, namespace string) {
		timeout := 2 * time.Minute
		if os.Getenv("DEPLOYER") == "k8s" {
			timeout = 5 * time.Minute
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		result, err := h.orchestrate(ctx, design, namespace)
		if err != nil {
			log.Printf("orchestration failed for namespace=%s: %v", namespace, err)
			if h.store != nil {
				_ = h.store.UpdateStatus(context.Background(), namespace, "failed")
			}
			failedTeardownAt := time.Now().Add(30 * time.Second)
			if h.scheduler != nil {
				h.scheduler.Schedule(namespace, failedTeardownAt)
			} else {
				go func(ns string) {
					time.Sleep(30 * time.Second)
					d, ok := h.deployers["db"]
					if !ok {
						d = &deployer.LocalDeployer{NodeType: "db"}
					}
					if tdErr := d.Teardown(context.Background(), ns); tdErr != nil {
						log.Printf("failed deployment teardown failed for namespace=%s: %v", ns, tdErr)
					}
				}(namespace)
			}
			return
		}

		runExpiresAt := time.Now().Add(9 * time.Minute)
		if h.store != nil {
			_ = h.store.UpdateStatus(context.Background(), namespace, "running")
			_ = h.store.UpdateExpiresAt(context.Background(), namespace, runExpiresAt)
			_ = h.store.UpdateMetricsSummary(context.Background(), namespace, map[string]any{
				"seeded": result.Seeded,
			})
		}
		if h.scheduler != nil {
			h.scheduler.Schedule(namespace, runExpiresAt)
		}
	}(design, namespace)
}

// orchestrate deploys nodes in dependency layers; nodes within a layer run in parallel.
// On any failure, it attempts to teardown already-deployed nodes (rollback).
func (h *Handler) orchestrate(
	ctx context.Context,
	design deployer.DeploymentDesign,
	namespace string,
) (*deployer.DeploymentResult, error) {

	var deployed []deployer.DeploymentNode

	layers, err := deployer.DeploymentLayers(design)
	if err != nil {
		return nil, fmt.Errorf("topology: %w", err)
	}

	for _, layer := range layers {
		errs := make([]error, len(layer))
		var wg sync.WaitGroup
		for i := range layer {
			wg.Add(1)
			go func(idx int, node deployer.DeploymentNode) {
				defer wg.Done()
				d, ok := h.deployers[node.Type]
				if !ok {
					d = &deployer.LocalDeployer{NodeType: node.Type}
				}
				cfg := deployer.DeployConfig{
					Namespace: namespace,
					Node:      node,
					Design:    design,
				}
				if err := d.Deploy(ctx, cfg); err != nil {
					errs[idx] = fmt.Errorf("deploy node %s: %w", node.Name, err)
				}
			}(i, layer[i])
		}
		wg.Wait()

		for _, e := range errs {
			if e != nil {
				log.Printf("deploy failed, rolling back %d nodes", len(deployed))
				h.rollback(context.Background(), deployed, namespace)
				return nil, e
			}
		}
		deployed = append(deployed, layer...)
	}

	seeded := false
	if h.seeder != nil {
		if err := h.seeder.SeedDatabase(ctx, namespace, design.Template); err != nil {
			log.Printf("seeding failed for namespace=%s (continuing deployment): %v", namespace, err)
		} else {
			seeded = true
		}
	}

	return &deployer.DeploymentResult{
		DeploymentID: namespace,
		Namespace:    namespace,
		Status:       "running",
		Seeded:       seeded,
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
	assessment := deployer.AssessTopology(d)
	if assessment.Verdict == deployer.VerdictInvalid {
		return fmt.Errorf("%s", assessment.Explanation)
	}
	return nil
}

func designHasLoadBalancer(design map[string]any) bool {
	if design == nil {
		return false
	}
	raw, ok := design["nodes"]
	if !ok || raw == nil {
		return false
	}
	nodes, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, item := range nodes {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if t, _ := m["type"].(string); t == "lb" {
			return true
		}
	}
	return false
}

// handleStartLoadGen starts or updates load generation for a deployment.
func (h *Handler) handleStartLoadGen(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}

	if h.loadgen == nil {
		writeError(w, http.StatusServiceUnavailable, "load generator unavailable")
		return
	}

	var profile loadgen.LoadProfile
	err := json.NewDecoder(r.Body).Decode(&profile)
	if err != nil {
		if errors.Is(err, io.EOF) {
			profile = loadgen.DefaultProfile()
		} else {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
			return
		}
	}

	var row store.DeploymentRow
	var haveRow bool
	if h.store != nil {
		if rrow, gerr := h.store.GetDeployment(r.Context(), namespace); gerr == nil {
			row = rrow
			haveRow = true
		}
	}
	if profile.Template == "" && haveRow {
		profile.Template = row.Template
	}
	if profile.Template == "" {
		writeError(w, http.StatusBadRequest, "load generator requires a template — ensure the deployment has a valid template set")
		return
	}

	if specs, ok := loadgen.TemplateEndpoints[profile.Template]; !ok || len(specs) == 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("load generator does not yet support template %q — it has no endpoint definitions", profile.Template))
		return
	}

	profile = loadgen.NormalizeProfile(profile)

	if profile.BaseURL == "" && haveRow && designHasLoadBalancer(row.Design) {
		profile.BaseURL = fmt.Sprintf("http://lb.%s.svc.cluster.local:80", namespace)
	}

	if err := h.loadgen.Start(namespace, profile); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if h.eventBus != nil {
		h.eventBus.Emit(namespace, metrics.Event{
			Timestamp:   time.Now(),
			Type:        "action",
			Source:      "loadgen",
			Title:       "Load generator started",
			Description: fmt.Sprintf("template=%s rps=%d durationSeconds=%d", profile.Template, profile.RPS, profile.DurationSeconds),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":          "started",
		"template":        profile.Template,
		"rps":             profile.RPS,
		"distribution":    profile.Distribution,
		"durationSeconds": profile.DurationSeconds,
	})
}

// handleStopLoadGen stops load generation for a deployment.
func (h *Handler) handleStopLoadGen(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}

	if h.loadgen == nil {
		writeError(w, http.StatusServiceUnavailable, "load generator unavailable")
		return
	}

	h.loadgen.Stop(namespace)
	if h.eventBus != nil {
		h.eventBus.Emit(namespace, metrics.Event{
			Timestamp:   time.Now(),
			Type:        "action",
			Source:      "loadgen",
			Title:       "Load generator stopped",
			Description: "Traffic generation halted for this deployment",
		})
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleDeleteDeployment(w http.ResponseWriter, r *http.Request) {
	//PathValue is Go 1.22+:extracts {id} from the URL pattern.
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	// Stop load generator if running (prevents requests to a dying namespace)
	if h.loadgen != nil {
		h.loadgen.Stop(namespace)
	}
	if h.scheduler != nil {
		h.scheduler.Cancel(namespace)
	}
	// Persist final metrics summary before teardown (read-modify-write preserves 'seeded' and other fields).
	if h.metricsCollector != nil && h.store != nil {
		tpl := ""
		row, rowErr := h.store.GetDeployment(r.Context(), namespace)
		if rowErr == nil {
			tpl = row.Template
		}
		dm, err := h.metricsCollector.Collect(r.Context(), namespace, tpl)
		if err == nil && dm != nil {
			summary := make(map[string]any)
			if rowErr == nil && row.MetricsSummary != nil {
				for k, v := range row.MetricsSummary {
					summary[k] = v
				}
			}
			for k, v := range h.metricsCollector.PeakSummaryForNamespace(namespace, dm) {
				summary[k] = v
			}
			if hist := h.metricsCollector.DownsampledSessionHistory(namespace); len(hist) > 0 {
				summary["session_history"] = hist
			}
			summary["terminated_at"] = time.Now().Format(time.RFC3339)
			if rowErr == nil {
				summary["actual_duration_seconds"] = int(time.Since(row.CreatedAt).Seconds())
			}
			_ = h.store.SetMetricsSummary(r.Context(), namespace, summary)
		}
	}
	if h.metricsCollector != nil {
		h.metricsCollector.Cleanup(namespace)
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
