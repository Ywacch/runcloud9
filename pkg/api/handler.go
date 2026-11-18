package api

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/admission"
	"github.com/ywacch/cloud9/pkg/clusterstate"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/diagnostics"
	"github.com/ywacch/cloud9/pkg/inference"
	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/loadgen/vu"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/metrics/hub"
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
	deployers            map[string]deployer.ComponentDeployer
	store                HandlerDeploymentStore
	scheduler            *teardown.Scheduler
	seeder               seederService
	metricsCollector     *metrics.MetricsCollector
	diagnostics          *diagnostics.Collector
	clientset            kubernetes.Interface
	loadgen              loadgen.LoadGenerator
	eventBus             *metrics.EventBus
	provisionMu          sync.Mutex
	emitProvisionMu      sync.Mutex
	provisionCancel      map[string]context.CancelFunc
	teardownOnce         sync.Once
	teardown             *teardownQueue
	door                 *provisionDoor
	admitMu              sync.Mutex
	roomBudget           *admission.Config
	maxDeployQueue       int
	queueTTL             time.Duration
	queueWake            chan struct{}
	provisionETA         *provisionETA
	chaosTTLMu           sync.Mutex
	chaosTTLTimers       map[string]*time.Timer
	inferenceClient      inferenceService
	inferenceTokenBudget *SessionTokenBudget
	actAs                *actAsValidator
	manifestRNG          func() int64
	lbEndpointsWatch     *deployer.LBEndpointsWatcher
	cpMetrics            *controlPlaneMetrics
	clusterState         *clusterstate.Cache
	lbEndpointView       deployer.EndpointView
	reconcileLBOpts      *deployer.ReconcileLBUpstreamOptions
}

// HandlerDeploymentStore is the store surface required by the HTTP handler.
type HandlerDeploymentStore interface {
	ListActiveDeployments(ctx context.Context, userID string) ([]store.ActiveDeploymentRow, error)
	ListUserDeploymentHistory(ctx context.Context, userID string, limit int) ([]store.DeploymentHistoryRow, error)
	ListQueuedDeployments(ctx context.Context) ([]store.QueuedDeploymentRow, error)
	CountQueuedDeployments(ctx context.Context) (int, error)
	CountUserDeploymentsSince(ctx context.Context, userID string, since time.Time) (int, error)
	ClaimQueued(ctx context.Context, namespace string) (bool, error)
	InsertAdmissionEvent(ctx context.Context, ev store.AdmissionEvent) error
	CreateDeployment(ctx context.Context, params store.CreateDeploymentParams) (string, error)
	SumActiveAdmissionWeights(ctx context.Context) (store.AdmissionOccupancy, error)
	UpdateStatus(ctx context.Context, namespace, status string) error
	ClaimStatus(ctx context.Context, namespace string, from []string, to string) (bool, error)
	ListDeploymentsByStatus(ctx context.Context, statuses []string) ([]store.DeploymentRow, error)
	UpdateExpiresAt(ctx context.Context, namespace string, expiresAt time.Time) error
	UpdateMetricsSummary(ctx context.Context, namespace string, patch map[string]any) error
	GetDeployment(ctx context.Context, namespace string) (store.DeploymentRow, error)
	GetStaleDeployments(ctx context.Context) ([]store.DeploymentRow, error)
	SetMetricsSummary(ctx context.Context, namespace string, summary map[string]any) error
	GetUser(ctx context.Context, userID string) (store.UserRecord, error)
}

type seederService interface {
	SeedDatabase(ctx context.Context, namespace, template string) error
	PreDownloadDump(ctx context.Context, template string) (string, error)
	SeedWithDump(ctx context.Context, namespace, template, dumpFile string) error
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
	store HandlerDeploymentStore,
	scheduler *teardown.Scheduler,
	seed *seeder.Seeder,
	metricsCollector *metrics.MetricsCollector,
	diagnosticsCollector *diagnostics.Collector,
	clientset kubernetes.Interface,
	lg loadgen.LoadGenerator,
	eventBus *metrics.EventBus,
) *Handler {
	var seedSvc seederService
	if seed != nil {
		seedSvc = seed
	}

	maxQueue, queueTTL, etaSec := queueConfigFromEnv()

	var door *provisionDoor
	if raw := strings.TrimSpace(os.Getenv("MAX_CONCURRENT_PROVISIONS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			door = newProvisionDoor(n, etaSec)
		} else {
			log.Printf("MAX_CONCURRENT_PROVISIONS invalid %q: %v (disabling doorway)", raw, err)
		}
	}

	var inferenceSvc inferenceService
	if inferenceURL := strings.TrimSpace(os.Getenv("INFERENCE_URL")); inferenceURL != "" {
		inferenceSvc = inference.NewClient(inferenceURL)
	}

	poolFetcher := &deploymentUserPoolFetcher{store: store, clientset: clientset}
	h := &Handler{
		deployers:            deployers,
		store:                store,
		scheduler:            scheduler,
		seeder:               seedSvc,
		metricsCollector:     metricsCollector,
		diagnostics:          diagnosticsCollector,
		clientset:            clientset,
		loadgen:              lg,
		eventBus:             eventBus,
		actAs:                newActAsValidator(poolFetcher, time.Now, 5*time.Minute),
		provisionCancel:      make(map[string]context.CancelFunc),
		door:                 door,
		roomBudget:           admission.ConfigFromEnv(),
		maxDeployQueue:       maxQueue,
		queueTTL:             queueTTL,
		queueWake:            make(chan struct{}, 1),
		provisionETA:         newProvisionETA(etaSec),
		inferenceClient:      inferenceSvc,
		inferenceTokenBudget: NewSessionTokenBudget(),
		manifestRNG:          randomManifestRNGSeed,
		lbEndpointsWatch:     deployer.NewLBEndpointsWatcher(),
		cpMetrics:            controlPlaneMetricsForHandler(),
	}
	if h.metricsCollector != nil && h.cpMetrics != nil {
		h.metricsCollector.SetMetricsQueryObserver(h.cpMetrics.observeMetricsQueries)
		h.metricsCollector.SetPromQueryObserver(h.cpMetrics.observePromQuery)
		h.metricsCollector.SetSignalCompareObserver(h.cpMetrics.observeSignalCompare)
		h.metricsCollector.SetCollectObserver(h.cpMetrics.observeCollectSeconds)
		h.metricsCollector.SetFrameBytesObserver(h.cpMetrics.observeFrameBytes)
		h.metricsCollector.SetSideWorkInflightObserver(h.cpMetrics.setSideWorkInflight)
		h.cpMetrics.RegisterRingGauge(func() float64 {
			return float64(h.metricsCollector.RingBytes())
		})
	}
	type storeRequestObserver interface {
		SetRequestObserver(func(string))
	}
	if sh, ok := store.(storeRequestObserver); ok && h.cpMetrics != nil {
		sh.SetRequestObserver(h.cpMetrics.observeStoreRequest)
	}
	return h
}

// HubMetricsObserver exposes control-plane counters for the metrics hub refresh loop.
func (h *Handler) HubMetricsObserver() hub.ObserveHub {
	if h == nil || h.cpMetrics == nil {
		return nil
	}
	return h.cpMetrics.HubMetricsObserver()
}

// SetClusterState attaches the shared informer cache for later read paths (P2.2+).
// Nil leaves today's API-server behavior.
func (h *Handler) SetClusterState(c *clusterstate.Cache) {
	if h == nil {
		return
	}
	if h.clusterState != nil && h.clusterState != c {
		h.clusterState.SetLBEndpointsNotify(nil)
	}
	h.clusterState = c
	if c != nil && clusterstate.LBEnabled(os.Getenv("CLUSTERSTATE_LB")) {
		h.lbEndpointView = c
		h.reconcileLBOpts = &deployer.ReconcileLBUpstreamOptions{EndpointView: c}
		c.SetLBEndpointsNotify(func(obj any) {
			if h.lbEndpointsWatch != nil {
				h.lbEndpointsWatch.HandleClusterEndpointsObject(obj)
			}
		})
		log.Println("clusterstate lb: on")
	} else {
		h.lbEndpointView = nil
		h.reconcileLBOpts = nil
		if c != nil {
			c.SetLBEndpointsNotify(nil)
		}
		log.Println("clusterstate lb: off")
	}
}

// RegisterRoutes attaches handlers to the mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	limiter, err := NewRequestLimiterFromEnv()
	if err != nil {
		log.Fatalf("rate limiter: %v", err)
	}
	auth := requireAuth
	rl := newRateLimitMiddleware(limiter)

	mux.Handle("POST /deployments", routeAuthExpensive(auth, rl, http.HandlerFunc(h.handleCreateDeployment)))
	mux.Handle("GET /deployments", routeAuth(auth, rl, http.HandlerFunc(h.handleListDeployments)))
	mux.Handle("GET /internal/fleet", routeAuth(auth, rl, http.HandlerFunc(h.handleFleet)))
	mux.Handle("GET /deployments/history", routeAuth(auth, rl, http.HandlerFunc(h.handleListDeploymentHistory)))
	mux.Handle("GET /me", routeAuth(auth, rl, http.HandlerFunc(h.handleGetCurrentUser)))
	mux.Handle("GET /me/engine-gates", routeAuth(auth, rl, http.HandlerFunc(h.handleEngineGates)))
	mux.Handle("GET /deployments/{id}", routeAuth(auth, rl, http.HandlerFunc(h.handleGetDeployment)))
	mux.Handle("DELETE /deployments/{id}", routeAuthRelease(auth, rl, http.HandlerFunc(h.handleDeleteDeployment)))
	mux.Handle("GET /deployments/{id}/ai-context", routeAuth(auth, rl, http.HandlerFunc(h.handleAIContext)))
	mux.Handle("GET /deployments/{id}/metrics", routeAuth(auth, rl, http.HandlerFunc(h.handleMetricsStream)))
	mux.Handle("GET /deployments/{id}/chat", routeAuth(auth, rl, http.HandlerFunc(h.handleChatStream)))
	mux.Handle("GET /deployments/{id}/logs", routeAuth(auth, rl, http.HandlerFunc(h.handleLogsStream)))
	mux.Handle("GET /deployments/{id}/proxy/{component}", routeAuth(auth, rl, http.HandlerFunc(h.handleProxy)))
	mux.Handle("GET /deployments/{id}/proxy/{component}/{path...}", routeAuth(auth, rl, http.HandlerFunc(h.handleProxy)))
	mux.Handle("POST /deployments/{id}/proxy/{component}", routeAuth(auth, rl, http.HandlerFunc(h.handleProxy)))
	mux.Handle("POST /deployments/{id}/proxy/{component}/{path...}", routeAuth(auth, rl, http.HandlerFunc(h.handleProxy)))
	mux.Handle("OPTIONS /deployments/{id}/proxy/{component}", routeAuth(auth, rl, http.HandlerFunc(h.handleProxy)))
	mux.Handle("OPTIONS /deployments/{id}/proxy/{component}/{path...}", routeAuth(auth, rl, http.HandlerFunc(h.handleProxy)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/capabilities", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/tables", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/schema/{param}", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/rows/{param}", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/indexes", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/keys", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/stats", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/key/{param}", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/queues", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("GET /deployments/{id}/introspect/{component}/peek/{param}", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/capabilities", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/tables", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/schema/{param}", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/rows/{param}", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/indexes", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/keys", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/stats", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/key/{param}", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/queues", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("OPTIONS /deployments/{id}/introspect/{component}/peek/{param}", routeAuth(auth, rl, http.HandlerFunc(h.handleIntrospect)))
	mux.Handle("POST /deployments/{id}/loadgen", routeAuthExpensive(auth, rl, http.HandlerFunc(h.handleStartLoadGen)))
	mux.Handle("PATCH /deployments/{id}/loadgen", routeAuth(auth, rl, http.HandlerFunc(h.handleUpdateLoadGen)))
	mux.Handle("PATCH /deployments/{id}/replica-lag", routeAuth(auth, rl, http.HandlerFunc(h.handleUpdateReplicaLag)))
	mux.Handle("DELETE /deployments/{id}/loadgen", routeAuth(auth, rl, http.HandlerFunc(h.handleStopLoadGen)))
	mux.Handle("POST /deployments/{id}/chaos", routeAuthExpensive(auth, rl, http.HandlerFunc(h.handleChaosApply)))
	mux.Handle("DELETE /deployments/{id}/chaos", routeAuth(auth, rl, http.HandlerFunc(h.handleChaosHeal)))
	mux.Handle("GET /deployments/{id}/chaos", routeAuth(auth, rl, http.HandlerFunc(h.handleChaosList)))
	mux.Handle("GET /health", routePublic(rl, http.HandlerFunc(h.handleHealth)))
	mux.Handle("GET /templates", routePublic(rl, http.HandlerFunc(h.handleGetTemplates)))
	mux.Handle("GET /templates/{id}/loadgen", routePublic(rl, http.HandlerFunc(h.handleGetTemplateLoadgen)))
	mux.Handle("GET /meta/resource-profiles", routePublic(rl, http.HandlerFunc(h.handleGetResourceProfiles)))
	mux.Handle("GET /meta/topology-rules", routePublic(rl, http.HandlerFunc(h.handleGetTopologyRules)))
	mux.Handle("GET /meta/rules", routePublic(rl, http.HandlerFunc(h.handleGetRules)))
	mux.Handle("GET /meta/jobs", routePublic(rl, http.HandlerFunc(h.handleGetMetaJobs)))
	mux.Handle("POST /designs/plan", routeAuth(auth, rl, http.HandlerFunc(h.handleDesignPlan)))
	mux.Handle("GET /meta/inferred-mechanism-promotions", routePublic(rl, http.HandlerFunc(h.handleListInferredMechanismPromotions)))
	if h.cpMetrics != nil {
		mux.Handle("GET /metrics", h.cpMetrics.metricsHandler())
	}
}

// handleMetricsStream upgrades the HTTP connection to WebSocket and streams
// deployment metrics until the client disconnects or the deployment expires.
func (h *Handler) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	row := h.requireOwnership(w, r, namespace)
	if row == nil {
		return
	}

	if wantsSSE(r) {
		if h.cpMetrics != nil {
			h.cpMetrics.incStreamConnection("sse", "metrics")
			defer h.cpMetrics.decStreamConnection("sse", "metrics")
		}
		h.streamMetricsSSE(w, r, row, metricsSSEOptions{})
		return
	}

	if h.cpMetrics != nil {
		h.cpMetrics.incStreamConnection("ws", "metrics")
		defer h.cpMetrics.decStreamConnection("ws", "metrics")
	}
	h.streamMetricsWebSocket(w, r, row)
}

func (h *Handler) handleGetDeployment(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	row := h.requireOwnership(w, r, namespace)
	if row == nil {
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
	h.attachWaitFields(resp, r.Context(), namespace, row.Status)
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

	userID := userIDFromContext(r.Context())

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
	if h.requireOwnership(w, r, namespace) == nil {
		return
	}

	// UNDERSTAND: if clientset is nil (local dev without K8s),
	// return 503 rather than panic. Log streaming requires a real cluster.
	if h.clientset == nil {
		writeError(w, http.StatusServiceUnavailable, "log streaming requires K8s deployer")
		return
	}

	if wantsSSE(r) {
		if h.cpMetrics != nil {
			h.cpMetrics.incStreamConnection("sse", "logs")
			defer h.cpMetrics.decStreamConnection("sse", "logs")
		}
		h.streamLogsSSE(w, r, namespace)
		return
	}

	if h.cpMetrics != nil {
		h.cpMetrics.incStreamConnection("ws", "logs")
		defer h.cpMetrics.decStreamConnection("ws", "logs")
	}
	h.streamLogsWebSocket(w, r, namespace)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func (h *Handler) handleGetTemplates(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(templates.Registry())
}

type templateLoadgenResponse struct {
	templates.LoadgenConfigView
	Conversations *loadgen.VUConfig `json:"conversations,omitempty"`
}

func (h *Handler) handleGetTemplateLoadgen(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	view, ok := templates.LoadgenConfig(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("template %q not found or has no loadgen config", id))
		return
	}
	resp := templateLoadgenResponse{LoadgenConfigView: view}
	if actions, ok := loadgen.VUActionsFromTemplate(id); ok {
		names := make(map[string]struct{}, len(actions))
		for k := range actions {
			names[k] = struct{}{}
		}
		if cfg, has := vu.DefaultConversations(names); has {
			resp.Conversations = &cfg
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleGetResourceProfiles(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(deployer.NewResourceCatalogResponse())
}

func (h *Handler) handleGetTopologyRules(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(deployer.NewTopologyRulesResponse())
}

func (h *Handler) handleCreateDeployment(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromContext(r.Context())
	limitJSONBody(w, r)

	var design deployer.DeploymentDesign
	if err := json.NewDecoder(r.Body).Decode(&design); err != nil {
		if isBodyTooLarge(err) {
			writeError(w, http.StatusBadRequest, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	deployer.NormalizeDesign(&design)
	deployer.EnsureLoadBalancerForReplicas(&design)

	if err := validateDesign(design); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	tmpl := templates.GetTemplate(design.Template)
	if tmpl == nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("template %q is not registered — select a valid template", design.Template))
		return
	}
	if !tmpl.Deployable {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("template %q is not yet deployable — it can only be used in simulation mode", design.Template))
		return
	}

	if userID != "dev-user" && !emailVerifiedFromContext(r.Context()) {
		writeErrorWithReason(w, http.StatusForbidden, "confirm your email before deploying", "email_unconfirmed")
		return
	}

	userRecord, err := h.loadUserRecord(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load user")
		return
	}
	if !tmpl.Available && !userRecord.IsInternal {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("template %q is not yet available", design.Template))
		return
	}
	effectiveTier := store.ResolveEffectiveTier(userRecord)
	limits := store.GetTierLimits(effectiveTier)
	resourceQuotaTier := store.ResourceQuotaTier(effectiveTier)
	sessionMinutes := store.ResolveSessionMinutes(effectiveTier, design.SessionMinutes)

	if !limits.AllTemplates && tmpl.Premium {
		writeError(w, http.StatusForbidden, "premium templates require an active subscription")
		return
	}

	if err := deployer.ValidateTierTopology(design, limits.TopologyLimits()); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}

	if err := deployer.ValidateInstanceSizeForTier(design, limits.MaxInstanceSize); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
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
			"assessment":   deployer.AssessmentMap(assessment, nil),
		})
		return
	}

	deployWarnings := deployer.TemplateCapabilityWarnings(design)
	deployWarnings = append(deployWarnings, deployer.RuntimeContractWarnings(design)...)

	if err := deployer.ValidateResourceBudget(design, resourceQuotaTier); err != nil {
		var budgetErr *deployer.ResourceBudgetError
		if errors.As(err, &budgetErr) {
			writeResourceBudgetError(w, budgetErr)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	namespace := generateNamespace(design.Template, userID)
	sessionExpiresAt := time.Now().Add(time.Duration(sessionMinutes) * time.Minute)

	status := statusProvisioning
	queuedAhead := 0
	if h.store != nil {
		allowBurst := strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Cloud9-Batch-Launch")), "1")
		h.admitMu.Lock()
		var admitErr error
		status, queuedAhead, admitErr = h.admitAndCreate(r.Context(), userID, limits.MaxConcurrent, limits.MaxSessionsPerMonth, design, resourceQuotaTier, namespace, sessionExpiresAt, allowBurst)
		h.admitMu.Unlock()
		if admitErr != nil {
			log.Printf("admit create deployment: %v", admitErr)
			var queueFull *admitQueueFullError
			if errors.As(admitErr, &queueFull) {
				h.recordAdmissionEvent(r.Context(), store.AdmissionEvent{
					UserID:        userID,
					Template:      design.Template,
					Kind:          store.AdmissionKindQueueFull,
					QueuePosition: intPtr(queueFull.depth),
					Paid:          h.admissionPaid(r.Context(), userID),
				})
			}
			writeAdmitError(w, admitErr)
			return
		}
	}

	resp := map[string]any{
		"deploymentId": namespace,
		"namespace":    namespace,
		"status":       status,
		"assessment":   deployer.AssessmentMap(assessment, deployWarnings),
	}
	if status == statusQueued || status == statusProvisioning {
		h.attachWaitFields(resp, r.Context(), namespace, status)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)

	if h.store != nil {
		h.finishAdmit(r.Context(), userID, design, namespace, status, queuedAhead)
	}

	if status == statusQueued {
		h.wakeDeployQueue()
		return
	}

	// Continue orchestration in the background after returning the response.
	go h.runBackgroundProvision(design, namespace, userID, resourceQuotaTier, sessionExpiresAt, sessionMinutes)
}

// orchestrate deploys nodes in dependency layers; nodes within a layer run in parallel.
// On any failure, it attempts to teardown already-deployed nodes (rollback).
// afterSeedRebaseline snapshots postgres session totals after seed. Tests replace it.
var afterSeedRebaseline = func(c *metrics.MetricsCollector, namespace string) {
	if c == nil {
		return
	}
	go c.WaitAndRebaselinePostgresSession(context.Background(), namespace)
}

func (h *Handler) orchestrate(
	ctx context.Context,
	design deployer.DeploymentDesign,
	namespace string,
	resourceQuotaTier string,
	sessionExpiresAt time.Time,
) (*deployer.DeploymentResult, error) {

	var deployed []deployer.DeploymentNode

	if h.clientset != nil {
		if err := deployer.EnsureNamespaceResourceQuota(ctx, h.clientset, namespace, design, resourceQuotaTier, sessionExpiresAt); err != nil {
			return nil, fmt.Errorf("resource quota: %w", err)
		}
	}

	layers, err := deployer.DeploymentLayers(design)
	if err != nil {
		return nil, fmt.Errorf("topology: %w", err)
	}

	componentStatus := make(map[string]string, len(design.Nodes))
	for _, n := range design.Nodes {
		componentStatus[n.ID] = "pending"
	}
	seedingStatus := statusOrSkipped(h.seeder, "pending")
	loadgenStatus := loadgenStatusPending(h.clientset)
	var provisionMu sync.Mutex
	replicaStatus := make(map[string][]deployer.PodWaitProgress)
	deployFailDetail := ""

	emitProvision := func(phase string, components map[string]string) {
		provisionMu.Lock()
		seeding := seedingStatus
		loadgen := loadgenStatus
		replicas := cloneReplicaStatus(replicaStatus)
		failDetail := deployFailDetail
		provisionMu.Unlock()
		detail := deployer.ComputeProvisionDetailCode(seeding, replicas, phase, failDetail)
		h.emitProvisionStatus(namespace, phase, components, seeding, loadgen, replicas, detail)
	}
	emitProvision("deploying", componentStatus)

	asyncRebaseline := false
	if h.seeder != nil && h.metricsCollector != nil && designHasDBNode(design) {
		h.metricsCollector.HoldPostgresSession(namespace)
		defer func() {
			if !asyncRebaseline {
				h.metricsCollector.OpenPostgresSession(namespace)
			}
		}()
	}

	type dumpResult struct {
		path string
		err  error
	}
	dumpReady := make(chan dumpResult, 1)
	if h.seeder != nil {
		go func() {
			path, err := h.seeder.PreDownloadDump(ctx, design.Template)
			dumpReady <- dumpResult{path: path, err: err}
		}()
	}

	var seeded bool
	startedSeed := false

	for _, layer := range layers {
		for _, node := range layer {
			componentStatus[node.ID] = "deploying"
		}
		emitProvision("deploying", componentStatus)

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
					ExpiresAt: sessionExpiresAt,
					OnWaitProgress: func(nodeID string, pods []deployer.PodWaitProgress) {
						provisionMu.Lock()
						replicaStatus[nodeID] = pods
						comps := cloneComponentStatus(componentStatus)
						provisionMu.Unlock()
						emitProvision("deploying", comps)
					},
				}
				if h.seeder != nil && node.Type == "db" {
					cfg.AfterPrimaryReady = func(ctx context.Context) error {
						provisionMu.Lock()
						if startedSeed {
							provisionMu.Unlock()
							return nil
						}
						startedSeed = true
						seedingStatus = "seeding"
						comps := cloneComponentStatus(componentStatus)
						provisionMu.Unlock()
						emitProvision("deploying", comps)

						res := <-dumpReady
						if res.err != nil {
							log.Printf("seeding failed for namespace=%s (continuing deployment): seed pre-download failed: %v", namespace, res.err)
							provisionMu.Lock()
							seedingStatus = "failed"
							provisionMu.Unlock()
							if h.metricsCollector != nil {
								h.metricsCollector.OpenPostgresSession(namespace)
							}
							return nil
						}
						if res.path == "" {
							log.Printf("seeding failed for namespace=%s (continuing deployment): seed dump not available", namespace)
							provisionMu.Lock()
							seedingStatus = "failed"
							provisionMu.Unlock()
							if h.metricsCollector != nil {
								h.metricsCollector.OpenPostgresSession(namespace)
							}
							return nil
						}
						if err := h.seeder.SeedWithDump(ctx, namespace, design.Template, res.path); err != nil {
							log.Printf("seeding failed for namespace=%s (continuing deployment): %v", namespace, err)
							provisionMu.Lock()
							seedingStatus = "failed"
							provisionMu.Unlock()
							if h.metricsCollector != nil {
								h.metricsCollector.OpenPostgresSession(namespace)
							}
							return nil
						}
						provisionMu.Lock()
						seedingStatus = "done"
						seeded = true
						comps = cloneComponentStatus(componentStatus)
						provisionMu.Unlock()
						emitProvision("deploying", comps)
						if h.metricsCollector != nil {
							asyncRebaseline = true
							afterSeedRebaseline(h.metricsCollector, namespace)
						}
						return nil
					}
				}
				if err := d.Deploy(ctx, cfg); err != nil {
					provisionMu.Lock()
					deployFailDetail = deployer.ProvisionDetailFromDeployError(err)
					provisionMu.Unlock()
					errs[idx] = fmt.Errorf("deploy node %s: %w", node.Name, err)
					return
				}
				provisionMu.Lock()
				componentStatus[node.ID] = "ready"
				comps := cloneComponentStatus(componentStatus)
				provisionMu.Unlock()
				emitProvision("deploying", comps)
			}(i, layer[i])
		}
		wg.Wait()

		for _, e := range errs {
			if e != nil {
				for i := range layer {
					if errs[i] != nil {
						componentStatus[layer[i].ID] = "failed"
					}
				}
				provisionMu.Lock()
				seedingStatus = statusOrSkipped(h.seeder, "failed")
				provisionMu.Unlock()
				emitProvision("failed", componentStatus)
				log.Printf("deploy failed, rolling back %d nodes", len(deployed))
				h.rollback(context.Background(), deployed, namespace)
				return nil, e
			}
		}
		deployed = append(deployed, layer...)
		for i, node := range layer {
			if errs[i] == nil {
				componentStatus[node.ID] = "ready"
			}
		}
		emitProvision("deploying", componentStatus)
	}

	if !startedSeed {
		seedingStatus = statusOrSkipped(h.seeder, "skipped")
	}

	if h.clientset != nil {
		if err := deployer.ReconcileLBUpstream(ctx, h.clientset, namespace, design, h.reconcileLBOpts); err != nil {
			log.Printf("lb upstream reconcile failed namespace=%s: %v", namespace, err)
		}
		var epView deployer.EndpointView
		if h.lbEndpointView != nil {
			epView = h.lbEndpointView
		}
		h.lbEndpointsWatch.Start(h.clientset, namespace, design, epView)
	}

	if h.clientset != nil {
		loadgenStatus = "ready"
		if err := deployer.DeployLoadgen(ctx, h.clientset, namespace, resourceQuotaTier); err != nil {
			log.Printf("loadgen pod deploy failed for namespace=%s (load testing unavailable): %v", namespace, err)
			loadgenStatus = "failed"
		}
	}

	emitProvision("running", componentStatus)

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

func (h *Handler) emitProvisionStatus(
	namespace, phase string,
	components map[string]string,
	seeding, loadgen string,
	replicaStatus map[string][]deployer.PodWaitProgress,
	provisionDetailCode string,
) {
	if h.store == nil {
		return
	}
	// Layer Deploy goroutines emit concurrently; serialize Get+merge+Update so
	// we never range a shared MetricsSummary map while another write patches it.
	h.emitProvisionMu.Lock()
	defer h.emitProvisionMu.Unlock()
	summary := map[string]any{
		"provision_phase":       phase,
		"seeding_status":        seeding,
		"loadgen_status":        loadgen,
		"provision_detail_code": provisionDetailCode,
	}
	c := make(map[string]string, len(components))
	for k, v := range components {
		c[k] = v
	}
	summary["component_status"] = c
	if len(replicaStatus) > 0 {
		summary["replica_status"] = replicaStatusToSummary(replicaStatus)
	}

	if row, err := h.store.GetDeployment(context.Background(), namespace); err == nil && row.MetricsSummary != nil {
		merged := make(map[string]any, len(row.MetricsSummary)+len(summary))
		for k, v := range row.MetricsSummary {
			merged[k] = v
		}
		for k, v := range summary {
			merged[k] = v
		}
		_ = h.store.UpdateMetricsSummary(context.Background(), namespace, merged)
		return
	}
	_ = h.store.UpdateMetricsSummary(context.Background(), namespace, summary)
}

func cloneComponentStatus(components map[string]string) map[string]string {
	out := make(map[string]string, len(components))
	for k, v := range components {
		out[k] = v
	}
	return out
}

func cloneReplicaStatus(in map[string][]deployer.PodWaitProgress) map[string][]deployer.PodWaitProgress {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]deployer.PodWaitProgress, len(in))
	for nodeID, pods := range in {
		copied := make([]deployer.PodWaitProgress, len(pods))
		copy(copied, pods)
		out[nodeID] = copied
	}
	return out
}

func replicaStatusToSummary(replicaStatus map[string][]deployer.PodWaitProgress) map[string]any {
	out := make(map[string]any, len(replicaStatus))
	for nodeID, pods := range replicaStatus {
		rows := make([]map[string]any, len(pods))
		for i, p := range pods {
			rows[i] = map[string]any{
				"ordinal":   p.Ordinal,
				"ready":     p.Ready,
				"wait_code": p.WaitCode,
			}
		}
		out[nodeID] = rows
	}
	return out
}

func designHasDBNode(design deployer.DeploymentDesign) bool {
	for _, n := range design.Nodes {
		if n.Type == "db" {
			return true
		}
	}
	return false
}

func statusOrSkipped(s seederService, status string) string {
	if s == nil {
		return "skipped"
	}
	return status
}

func loadgenStatusPending(clientset kubernetes.Interface) string {
	if clientset != nil {
		return "pending"
	}
	return "skipped"
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
	tmpl := templates.GetTemplate(d.Template)
	if tmpl != nil {
		if err := deployer.ValidateRuntimeContract(d, tmpl); err != nil {
			return err
		}
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
		if t, _ := m["type"].(string); t == "lb" || t == "load_balancer" {
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
	var row *store.DeploymentRow
	if h.store != nil {
		row = h.requireOwnership(w, r, namespace)
		if row == nil {
			return
		}
	}

	if h.loadgen == nil {
		writeError(w, http.StatusServiceUnavailable, "load generator unavailable")
		return
	}
	limitJSONBody(w, r)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var profile loadgen.LoadProfile
	err := dec.Decode(&profile)
	if err != nil {
		if isBodyTooLarge(err) {
			writeError(w, http.StatusBadRequest, "request body too large")
			return
		}
		if errors.Is(err, io.EOF) {
			// Zero profile: template comes from deployment row below; NormalizeProfile fills RPS (template-aware).
			profile = loadgen.LoadProfile{}
		} else {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
			return
		}
	}

	if profile.Template == "" && row != nil {
		profile.Template = row.Template
	}
	if profile.Template == "" {
		writeError(w, http.StatusBadRequest, "load generator requires a template — ensure the deployment has a valid template set")
		return
	}

	if !templates.HasLoadgenEndpoints(profile.Template) {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("load generator does not yet support template %q — it has no endpoint definitions", profile.Template))
		return
	}

	profile, err = loadgen.ValidateAndNormalizeProfile(profile)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if !h.validateLoadgenRPSForRequest(w, r, profile.RPS) {
		return
	}

	if profile.BaseURL == "" && row != nil && designHasLoadBalancer(row.Design) {
		profile.BaseURL = fmt.Sprintf("http://lb.%s.svc.cluster.local:80", namespace)
	}

	profile = h.applyVUObserverMaxThreadAge(profile, row)

	if err := h.loadgen.Start(r.Context(), namespace, profile); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	h.emitVUGapWarningIfNeeded(namespace, profile, row)

	h.stampRunManifestLoadProfile(r.Context(), namespace, profile)

	if h.eventBus != nil {
		h.eventBus.Emit(namespace, metrics.Event{
			Timestamp:   time.Now(),
			Type:        "action",
			Source:      "loadgen",
			Kind:        "loadgen_start",
			Visibility:  "all",
			Title:       "Load generator started",
			Description: fmt.Sprintf("template=%s rps=%d durationSeconds=%d", profile.Template, profile.RPS, profile.DurationSeconds),
			Detail: map[string]string{
				"template":        profile.Template,
				"rps":             strconv.Itoa(profile.RPS),
				"durationSeconds": strconv.FormatInt(profile.DurationSeconds, 10),
			},
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
	if h.store != nil && h.requireOwnership(w, r, namespace) == nil {
		return
	}

	if h.loadgen == nil {
		writeError(w, http.StatusServiceUnavailable, "load generator unavailable")
		return
	}

	_ = h.loadgen.Stop(r.Context(), namespace)
	if h.eventBus != nil {
		h.eventBus.Emit(namespace, metrics.Event{
			Timestamp:   time.Now(),
			Type:        "action",
			Source:      "loadgen",
			Kind:        "loadgen_stop",
			Visibility:  "all",
			Title:       "Load generator stopped",
			Description: "Traffic generation halted for this deployment",
		})
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleUpdateLoadGen(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	if h.store != nil && h.requireOwnership(w, r, namespace) == nil {
		return
	}
	if h.loadgen == nil {
		writeError(w, http.StatusServiceUnavailable, "load generator unavailable")
		return
	}
	limitJSONBody(w, r)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body struct {
		RPS          *int           `json:"rps"`
		Distribution map[string]int `json:"distribution"`
	}
	if err := dec.Decode(&body); err != nil {
		if isBodyTooLarge(err) {
			writeError(w, http.StatusBadRequest, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	if body.RPS == nil && len(body.Distribution) == 0 {
		writeError(w, http.StatusBadRequest, "rps and/or distribution required")
		return
	}
	if body.RPS != nil {
		if !h.validateLoadgenRPSForRequest(w, r, *body.RPS) {
			return
		}
	}

	update := loadgen.LiveUpdate{RPS: body.RPS}
	if len(body.Distribution) > 0 {
		update.Distribution = body.Distribution
	}

	if err := h.loadgen.Update(r.Context(), namespace, update); err != nil {
		if errors.Is(err, loadgen.ErrNotRunning) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if h.eventBus != nil {
		now := time.Now()
		if len(body.Distribution) > 0 {
			detail := formatDistributionDetail(body.Distribution)
			if body.RPS != nil {
				detail["rps"] = strconv.Itoa(*body.RPS)
			}
			h.eventBus.Emit(namespace, metrics.Event{
				Timestamp:   now,
				Type:        "action",
				Source:      "loadgen",
				Kind:        "loadgen_mix",
				Visibility:  "all",
				Title:       "Load generator mix updated",
				Description: fmt.Sprintf("distribution updated (%d endpoints)", len(body.Distribution)),
				Detail:      detail,
			})
		} else if body.RPS != nil {
			h.eventBus.Emit(namespace, metrics.Event{
				Timestamp:   now,
				Type:        "action",
				Source:      "loadgen",
				Kind:        "loadgen_rate",
				Visibility:  "all",
				Title:       "Load generator rate updated",
				Description: fmt.Sprintf("rps=%d", *body.RPS),
				Detail: map[string]string{
					"rps": strconv.Itoa(*body.RPS),
				},
			})
		}
	}

	resp := map[string]any{"status": "updated"}
	if body.RPS != nil {
		resp["rps"] = *body.RPS
	}
	if len(body.Distribution) > 0 {
		resp["distribution"] = body.Distribution
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func formatDistributionDetail(dist map[string]int) map[string]string {
	out := make(map[string]string, len(dist))
	for k, v := range dist {
		out[k] = strconv.Itoa(v)
	}
	return out
}

func (h *Handler) handleDeleteDeployment(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	defer func() {
		h.observeDeleteRequest(time.Since(started))
	}()
	//PathValue is Go 1.22+:extracts {id} from the URL pattern.
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	owned := h.requireOwnership(w, r, namespace)
	if owned == nil {
		return
	}

	// Queued rows never reached K8s — cancel without teardown and wake the queue.
	if owned.Status == statusQueued {
		if h.store != nil {
			_ = h.store.UpdateMetricsSummary(r.Context(), namespace, map[string]any{
				"terminated_at": time.Now().UTC().Format(time.RFC3339),
				"reason":        "user_cancel_queued",
			})
			_ = h.store.UpdateStatus(context.Background(), namespace, "terminated")
		}
		h.wakeDeployQueue()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 204 means the row is terminating or already terminal. The namespace may still exist.
	switch owned.Status {
	case statusTerminated:
		w.WriteHeader(http.StatusNoContent)
		return
	case statusTerminating:
		h.enqueueTeardown(namespace, time.Now(), teardownReasonDelete)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if h.store == nil {
		writeError(w, http.StatusInternalServerError, "deployment store unavailable")
		return
	}
	claimCtx, claimCancel := context.WithTimeout(context.Background(), 10*time.Second)
	ok, err := h.store.ClaimStatus(claimCtx, namespace, statusesThatMayTearDown, statusTerminating)
	claimCancel()
	if err != nil {
		log.Printf("delete claim %s: %v", namespace, err)
		writeError(w, http.StatusInternalServerError, "could not record teardown")
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.cancelProvision(namespace)
	if h.scheduler != nil {
		h.scheduler.Cancel(namespace)
	}
	h.enqueueTeardown(namespace, time.Now(), teardownReasonDelete)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) setProvisionCancel(namespace string, cancel context.CancelFunc) {
	h.provisionMu.Lock()
	defer h.provisionMu.Unlock()
	h.provisionCancel[namespace] = cancel
}

func (h *Handler) clearProvisionCancel(namespace string) {
	h.provisionMu.Lock()
	defer h.provisionMu.Unlock()
	delete(h.provisionCancel, namespace)
}

func (h *Handler) cancelProvision(namespace string) {
	h.provisionMu.Lock()
	cancel := h.provisionCancel[namespace]
	h.provisionMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// StopLBEndpointsWatch stops reconciling nginx upstreams for a terminated session.
func (h *Handler) StopLBEndpointsWatch(namespace string) {
	if h.lbEndpointsWatch != nil {
		h.lbEndpointsWatch.Stop(namespace)
	}
}

func (h *Handler) isDeploymentStatus(namespace, status string) bool {
	if h.store == nil {
		return false
	}
	row, err := h.store.GetDeployment(context.Background(), namespace)
	if err != nil {
		return false
	}
	return row.Status == status
}

// provisionWritesBlocked reports that teardown already owns the row.
// A later provision write must not replace terminating or terminated.
func (h *Handler) provisionWritesBlocked(namespace string) bool {
	if h.store == nil {
		return false
	}
	row, err := h.store.GetDeployment(context.Background(), namespace)
	if err != nil {
		return false
	}
	return row.Status == statusTerminating || row.Status == statusTerminated
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeErrorWithReason(w http.ResponseWriter, status int, msg, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":  msg,
		"reason": reason,
	})
}

func writeResourceBudgetError(w http.ResponseWriter, err *deployer.ResourceBudgetError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnprocessableEntity)
	_ = json.NewEncoder(w).Encode(err.JSON())
}

func (h *Handler) writeDiagnosticsSummary(namespace, url, outcome, collectionReason string, signatures []diagnostics.FailureSignature, partial bool, collectErr error) {
	if h.store == nil {
		return
	}
	signatureCodes := make([]string, 0, len(signatures))
	for _, sig := range signatures {
		signatureCodes = append(signatureCodes, sig.Code)
	}
	payload := map[string]any{
		"url":                url,
		"collected_at":       time.Now().UTC().Format(time.RFC3339),
		"outcome":            outcome,
		"schema_version":     2,
		"failure_signatures": signatureCodes,
		"partial":            partial,
		"collection_reason":  collectionReason,
	}
	if collectErr != nil {
		payload["error"] = collectErr.Error()
	}

	ctx := context.Background()
	row, err := h.store.GetDeployment(ctx, namespace)
	if err != nil || row.MetricsSummary == nil {
		if setErr := h.store.SetMetricsSummary(ctx, namespace, map[string]any{"diagnostics": payload}); setErr != nil {
			log.Printf("DDC: set diagnostics summary failed for %s: %v", namespace, setErr)
		}
		return
	}
	lifecycle := map[string]any{
		"created_at":       row.CreatedAt.UTC().Format(time.RFC3339),
		"duration_seconds": int(time.Since(row.CreatedAt).Seconds()),
	}
	if raw, ok := row.MetricsSummary["running_at"].(string); ok && raw != "" {
		lifecycle["running_at"] = raw
	}
	payload["lifecycle"] = lifecycle

	summary := make(map[string]any, len(row.MetricsSummary)+1)
	for k, v := range row.MetricsSummary {
		summary[k] = v
	}
	summary["diagnostics"] = payload
	if setErr := h.store.SetMetricsSummary(ctx, namespace, summary); setErr != nil {
		log.Printf("DDC: update diagnostics summary failed for %s: %v", namespace, setErr)
	}
}

// generateNamespace returns a DNS-safe namespace that avoids same-second collisions.
// Format: cloud9-{template}-{userPrefix}-{random8}
func generateNamespace(template, userID string) string {
	tpl := sanitizeNamespacePart(template, "template", 20)
	uid := sanitizeNamespacePart(userID, "dev-user", 8)

	b := make([]byte, 4) // 8 hex chars
	if _, err := crand.Read(b); err != nil {
		// Very unlikely; fallback keeps rollout unblocked while still being mostly unique.
		now := time.Now().UnixNano()
		return fmt.Sprintf("cloud9-%s-%s-%08x", tpl, uid, uint32(now&0xFFFFFFFF))
	}
	return fmt.Sprintf("cloud9-%s-%s-%x", tpl, uid, b)
}

func sanitizeNamespacePart(in, fallback string, maxLen int) string {
	s := strings.ToLower(strings.TrimSpace(in))
	var b strings.Builder
	b.Grow(len(s))
	prevDash := false
	for _, r := range s {
		allowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if allowed {
			b.WriteRune(r)
			prevDash = false
			continue
		}
		if !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = fallback
	}
	if len(out) > maxLen {
		out = strings.Trim(out[:maxLen], "-")
		if out == "" {
			out = fallback
		}
	}
	return out
}
