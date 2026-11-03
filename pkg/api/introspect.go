package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/introspect"
	"github.com/ywacch/cloud9/pkg/templates"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func (h *Handler) handleIntrospect(w http.ResponseWriter, r *http.Request) {
	setProxyCORS(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "only GET is allowed")
		return
	}

	namespace := r.PathValue("id")
	component := r.PathValue("component")
	param := r.PathValue("param")

	if namespace == "" || component == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id or component")
		return
	}

	row := h.requireOwnership(w, r, namespace)
	if row == nil {
		return
	}
	if row.Status == "failed" || row.Status == "terminated" {
		writeError(w, http.StatusGone, "deployment is not active")
		return
	}

	tmpl := templates.GetTemplate(row.Template)
	if tmpl == nil {
		writeError(w, http.StatusNotFound, "template not found")
		return
	}
	introSpec, ok := tmpl.Introspection[component]
	if !ok {
		writeError(w, http.StatusNotFound, "component not configured for introspection")
		return
	}
	if component == "cache" {
		if eng := cacheEngineFromStoredDesign(row.Design); eng == "memcached" {
			introSpec.Engine = "memcached"
			introSpec.KeyPatterns = nil
		} else if introSpec.Engine == "redis" {
			if mq, hasMQ := tmpl.Introspection["mq"]; hasMQ {
				introSpec.KeyPatterns = introspect.AppendQueueStreamKeys(introSpec.KeyPatterns, mq.Queues)
			}
		}
	}

	action := introspectAction(r.URL.Path, namespace, component)
	if action == "" {
		writeError(w, http.StatusBadRequest, "unknown introspection action")
		return
	}

	if err := validateIntrospectAction(action, introSpec, param, r); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}

	target, err := parseIntrospectTarget(r.URL.Query().Get("target"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	layerID := strings.TrimSpace(r.URL.Query().Get("layerId"))

	host, err := resolveIntrospectHost(r.Context(), introSpec.Engine, component, namespace, target, layerID, row.Design, h.clientset)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	switch introSpec.Engine {
	case "postgres":
		h.handlePostgresIntrospect(ctx, w, host, action, param, introSpec, r)
	case "redis":
		h.handleRedisIntrospect(ctx, w, host, action, param, introSpec, r)
	case "memcached":
		h.handleMemcachedIntrospect(ctx, w, host, action, param, introSpec, r)
	case "rabbitmq":
		h.handleRabbitMQIntrospect(ctx, w, host, action, param, introSpec)
	default:
		writeError(w, http.StatusBadRequest, "unsupported engine")
	}
}

// parseIntrospectTarget normalizes the ?target= query param for introspection routing.
func parseIntrospectTarget(raw string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "primary":
		return "primary", nil
	case "replica":
		return "replica", nil
	default:
		return "", fmt.Errorf("unknown introspection target %q", raw)
	}
}

// resolveIntrospectHost maps engine + component to an in-cluster DNS host or pod IP.
func resolveIntrospectHost(ctx context.Context, engine, component, namespace, target, layerID string, design map[string]any, clientset kubernetes.Interface) (string, error) {
	if target == "replica" {
		switch engine {
		case "redis", "rabbitmq":
			return "", fmt.Errorf("replica target is not supported for %s", engine)
		case "postgres":
			if component != "db" {
				return "", fmt.Errorf("replica target only applies to db component")
			}
			if !designHasReadReplicas(design) {
				return "", fmt.Errorf("deployment has no read replicas configured")
			}
			if layerID != "" {
				return resolvePostgresReplicaPodHost(ctx, clientset, namespace, layerID)
			}
			return fmt.Sprintf("%s.%s.svc.cluster.local", deployer.DBReplicaServiceName, namespace), nil
		default:
			return "", fmt.Errorf("unsupported engine")
		}
	}
	if layerID != "" {
		return "", fmt.Errorf("layerId applies only to replica introspection target")
	}

	switch engine {
	case "postgres":
		svcHost, _, err := resolveProxyTarget(component, design)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s.%s.svc.cluster.local", svcHost, namespace), nil
	case "redis":
		if component != "cache" {
			return "", fmt.Errorf("redis introspection only supports cache component")
		}
		svcHost := resolveIntrospectRedisHost(design)
		return fmt.Sprintf("%s.%s.svc.cluster.local", svcHost, namespace), nil
	case "memcached":
		if component != "cache" {
			return "", fmt.Errorf("memcached introspection only supports cache component")
		}
		return fmt.Sprintf("cache.%s.svc.cluster.local", namespace), nil
	case "rabbitmq":
		if component != "mq" {
			return "", fmt.Errorf("rabbitmq introspection only supports mq component")
		}
		return fmt.Sprintf("mq.%s.svc.cluster.local", namespace), nil
	default:
		return "", fmt.Errorf("unsupported engine")
	}
}

// designHasReadReplicas reports whether the stored design has a db node with readReplicas >= 1.
func designHasReadReplicas(design map[string]any) bool {
	if design == nil {
		return false
	}
	nodes, ok := design["nodes"].([]any)
	if !ok {
		return false
	}
	for _, raw := range nodes {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if t, _ := m["type"].(string); t != "db" {
			continue
		}
		cfg, _ := m["config"].(map[string]any)
		if deployer.ParseReadReplicaCount(cfg) >= 1 {
			return true
		}
	}
	return false
}

func resolvePostgresReplicaPodHost(ctx context.Context, clientset kubernetes.Interface, namespace, layerID string) (string, error) {
	if clientset == nil {
		return "", fmt.Errorf("kubernetes client unavailable for replica pod %q", layerID)
	}
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, layerID, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("replica pod %q: %w", layerID, err)
	}
	if pod.Labels["app"] != "postgres-replica" {
		return "", fmt.Errorf("layerId %q is not a read replica pod", layerID)
	}
	if pod.Status.PodIP == "" {
		return "", fmt.Errorf("replica pod %q has no pod IP yet", layerID)
	}
	return pod.Status.PodIP, nil
}

func introspectAction(path, namespace, component string) string {
	prefix := fmt.Sprintf("/deployments/%s/introspect/%s/", namespace, component)
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	rest = strings.TrimSuffix(rest, "/")
	if rest == "" {
		return ""
	}
	parts := strings.Split(rest, "/")
	return parts[0]
}

func validateIntrospectAction(action string, spec introspect.ComponentSpec, param string, r *http.Request) error {
	if spec.Engine == "memcached" && action != "stats" && action != "capabilities" && action != "keys" && action != "key" {
		return fmt.Errorf("memcached introspection supports stats, keys, and key")
	}
	switch action {
	case "schema", "rows":
		if param == "" {
			return fmt.Errorf("table name required")
		}
		if !introspect.TableAllowed(param, spec.Tables) {
			return fmt.Errorf("table %q is not allowed", param)
		}
	case "key":
		if param == "" {
			return fmt.Errorf("key required")
		}
		if !introspect.KeyMatchesPatterns(param, spec.KeyPatterns) {
			return fmt.Errorf("key %q is not allowed", param)
		}
	case "keys":
		pattern := r.URL.Query().Get("pattern")
		if pattern == "" && len(spec.KeyPatterns) > 0 {
			pattern = spec.KeyPatterns[0]
		}
		if pattern == "" {
			return fmt.Errorf("pattern required")
		}
		if !introspect.PatternAllowed(pattern, spec.KeyPatterns) {
			return fmt.Errorf("pattern %q is not allowed", pattern)
		}
	case "peek":
		if param == "" {
			return fmt.Errorf("queue name required")
		}
		if !introspect.QueueAllowed(param, spec.Queues) {
			return fmt.Errorf("queue %q is not allowed", param)
		}
	case "queues", "capabilities", "stats":
		// ok
	default:
		// Unknown actions are rejected later by the engine handler.
	}
	return nil
}

func cacheEngineFromStoredDesign(design map[string]any) string {
	parsed, err := deployer.DesignFromMap(design)
	if err != nil {
		return "redis"
	}
	for _, n := range parsed.Nodes {
		if n.Type == "cache" {
			return deployer.CacheEngineFromNode(n)
		}
	}
	return "redis"
}

func (h *Handler) handlePostgresIntrospect(ctx context.Context, w http.ResponseWriter, host, action, table string, spec introspect.ComponentSpec, r *http.Request) {
	driver, err := introspect.NewPostgresDriver(ctx, host)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("postgres connect failed: %v", err))
		return
	}
	defer driver.Close()

	storage := driver
	switch action {
	case "capabilities":
		writeIntrospectJSON(w, map[string]any{"capabilities": storage.Capabilities()})
	case "tables":
		tables, err := storage.ListTables(ctx)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, filterTables(tables, spec.Tables))
	case "schema":
		cols, err := storage.GetSchema(ctx, table)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, cols)
	case "rows":
		opts := introspect.PageOpts{
			Page:  queryInt(r, "page", 1),
			Limit: queryInt(r, "limit", 50),
			Sort:  r.URL.Query().Get("sort"),
			Order: r.URL.Query().Get("order"),
		}
		result, err := storage.GetRows(ctx, table, opts)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, result)
	case "indexes":
		idx, err := storage.GetIndexes(ctx)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, idx)
	default:
		writeError(w, http.StatusNotFound, "unknown postgres action")
	}
}

func filterTables(tables []introspect.TableInfo, allowed []string) []introspect.TableInfo {
	out := make([]introspect.TableInfo, 0, len(allowed))
	for _, t := range tables {
		if !introspect.IsStudentTable(t.Name) {
			continue
		}
		if introspect.TableAllowed(t.Name, allowed) {
			out = append(out, t)
		}
	}
	return out
}

func (h *Handler) handleRedisIntrospect(ctx context.Context, w http.ResponseWriter, host, action, key string, spec introspect.ComponentSpec, r *http.Request) {
	driver, err := introspect.NewRedisDriver(ctx, host)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("redis connect failed: %v", err))
		return
	}
	defer driver.Close()

	cache := driver
	switch action {
	case "capabilities":
		writeIntrospectJSON(w, map[string]any{"capabilities": cache.Capabilities()})
	case "keys":
		pattern := r.URL.Query().Get("pattern")
		if pattern == "" && len(spec.KeyPatterns) > 0 {
			pattern = spec.KeyPatterns[0]
		}
		cursor, _ := strconv.ParseUint(r.URL.Query().Get("cursor"), 10, 64)
		count := queryInt(r, "count", 50)
		keys, next, err := cache.ScanKeys(ctx, pattern, cursor, count)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		payload := map[string]any{
			"keys":   keys,
			"cursor": next,
		}
		if keyCount, err := cache.CountStudentKeys(ctx); err == nil {
			payload["keyCount"] = keyCount
		}
		writeIntrospectJSON(w, payload)
	case "key":
		kv, err := cache.GetKeyValue(ctx, key)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, kv)
	case "stats":
		keyCount, err := cache.CountStudentKeys(ctx)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, map[string]any{"keyCount": keyCount})
	default:
		writeError(w, http.StatusNotFound, "unknown redis action")
	}
}

// memcachedKeyPatterns picks globs for a Memcached key list.
// A query pattern is one whitelist entry. An empty query uses every template pattern.
func memcachedKeyPatterns(query string, allowed []string) []string {
	query = strings.TrimSpace(query)
	if query != "" {
		return []string{query}
	}
	out := make([]string, len(allowed))
	copy(out, allowed)
	return out
}

func (h *Handler) handleMemcachedIntrospect(ctx context.Context, w http.ResponseWriter, host, action, key string, spec introspect.ComponentSpec, r *http.Request) {
	driver, err := introspect.NewMemcachedDriver(ctx, host)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("memcached connect failed: %v", err))
		return
	}

	switch action {
	case "capabilities":
		writeIntrospectJSON(w, map[string]any{"capabilities": driver.Capabilities()})
	case "stats":
		stats, err := driver.ServerStats(ctx)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, map[string]any{"stats": stats})
	case "keys":
		patterns := memcachedKeyPatterns(r.URL.Query().Get("pattern"), spec.KeyPatterns)
		limit := queryInt(r, "count", 50)
		keys, truncated, err := driver.ListKeys(ctx, patterns, limit, time.Now())
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, map[string]any{
			"keys":      keys,
			"cursor":    0,
			"truncated": truncated,
		})
	case "key":
		kv, ok, err := driver.GetKey(ctx, key)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		if !ok {
			writeError(w, http.StatusNotFound, "key not found")
			return
		}
		writeIntrospectJSON(w, kv)
	default:
		writeError(w, http.StatusNotFound, "unknown memcached action")
	}
}

func (h *Handler) handleRabbitMQIntrospect(ctx context.Context, w http.ResponseWriter, host, action, queue string, spec introspect.ComponentSpec) {
	driver, err := introspect.NewRabbitMQDriver(ctx, host)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("rabbitmq connect failed: %v", err))
		return
	}
	defer driver.Close()

	switch action {
	case "capabilities":
		writeIntrospectJSON(w, map[string]any{"capabilities": driver.Capabilities()})
	case "queues":
		queues, err := driver.ListQueues(ctx, spec.Queues)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, queues)
	case "peek":
		peek, err := driver.PeekQueue(ctx, queue)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, peek)
	default:
		writeError(w, http.StatusNotFound, "unknown rabbitmq action")
	}
}

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func writeIntrospectJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
