package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

var hopByHopResponseHeaders = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
}

func setProxyCORS(w http.ResponseWriter, r *http.Request) {
	applyCORSAllowlist(w, r, AllowedOriginsFromEnv(), "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Max-Age", "86400")
}

// proxyRequestID returns the client trace id or generates one for downstream propagation.
func proxyRequestID(r *http.Request) string {
	if id := strings.TrimSpace(r.Header.Get("X-Request-ID")); id != "" {
		return id
	}
	return uuid.NewString()
}

// proxyRESTHeaders builds outbound headers for the K8s service proxy request.
func proxyRESTHeaders(r *http.Request, requestID string) map[string]string {
	h := map[string]string{
		"X-Request-ID":   requestID,
		"X-Trace-Sample": "1",
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		h["Content-Type"] = ct
	}
	if accept := r.Header.Get("Accept"); accept != "" {
		h["Accept"] = accept
	}
	return h
}

func cacheServiceHTTPPort(design map[string]any) int {
	if design == nil {
		return 6379
	}
	nodes, ok := design["nodes"].([]any)
	if !ok {
		return 6379
	}
	for _, raw := range nodes {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if t, _ := m["type"].(string); t != "cache" {
			continue
		}
		strategy := "cache-aside"
		if cfg, ok := m["config"].(map[string]any); ok {
			if s, ok := cfg["strategy"].(string); ok && s != "" {
				strategy = s
			}
			if s, ok := cfg["cacheStrategy"].(string); ok && s != "" {
				strategy = s
			}
		}
		// Fold legacy read-through → write-through for port selection.
		switch strings.ToLower(strings.TrimSpace(strategy)) {
		case "write-through", "write-behind", "read-through":
			return 8081
		default:
			return 6379
		}
	}
	return 6379
}

// resolveIntrospectRedisHost returns the in-cluster Service name for Redis introspection.
// Proxy strategies (write-through, write-behind; legacy read-through) expose HTTP on Service "cache"
// and Redis on Service "redis-internal"; cache-aside uses Service "cache" for Redis directly.
func resolveIntrospectRedisHost(design map[string]any) string {
	if cacheServiceHTTPPort(design) == 8081 {
		return "redis-internal"
	}
	return "cache"
}

// resolveProxyTarget maps a URL component segment to the in-cluster Service DNS name and port.
// design is the stored deployment JSON (for cache proxy vs redis-only).
func resolveProxyTarget(component string, design map[string]any) (host string, port int, err error) {
	switch component {
	case "app-server":
		return "app-server", 8080, nil
	case "lb":
		return "lb", 80, nil
	case "db":
		return "db", 5432, nil
	case "mq":
		return "", 0, fmt.Errorf("mq management UI is not available (rabbitmq:3 base image)")
	case "cache":
		return "cache", cacheServiceHTTPPort(design), nil
	case "worker":
		return "", 0, fmt.Errorf("worker has no HTTP ClusterIP service in this stack")
	default:
		return "", 0, fmt.Errorf("unknown component %q", component)
	}
}

// handleProxy forwards HTTP to a component Service inside the deployment namespace.
// Routes: /deployments/{id}/proxy/{component} and /deployments/{id}/proxy/{component}/{path...}
func (h *Handler) handleProxy(w http.ResponseWriter, r *http.Request) {
	setProxyCORS(w, r)
	if r.Method == http.MethodOptions {
		w.Header().Set("X-Request-ID", proxyRequestID(r))
		w.WriteHeader(http.StatusNoContent)
		return
	}

	namespace := r.PathValue("id")
	component := r.PathValue("component")
	proxyPath := r.PathValue("path")

	if namespace == "" || component == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id or component")
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "only GET, POST, and OPTIONS are allowed via proxy")
		return
	}
	if h.clientset == nil {
		writeError(w, http.StatusServiceUnavailable, "kubernetes client unavailable for proxy")
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

	svcHost, svcPort, err := resolveProxyTarget(component, row.Design)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var body []byte
	if r.Body != nil {
		limitedBody := http.MaxBytesReader(w, r.Body, 1<<20)
		defer limitedBody.Close()
		raw, readErr := io.ReadAll(limitedBody)
		if readErr != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", readErr))
			return
		}
		body = raw
	}

	// Route via K8s API server service proxy so local-dev (outside cluster DNS) also works.
	svcTarget := fmt.Sprintf("%s:%d", svcHost, svcPort)
	proxyPath = strings.TrimPrefix(proxyPath, "/")
	restReq := h.clientset.CoreV1().RESTClient().
		Verb(r.Method).
		Namespace(namespace).
		Resource("services").
		Name(svcTarget).
		SubResource("proxy")
	if proxyPath != "" {
		restReq = restReq.Suffix(proxyPath)
	}
	if len(body) > 0 {
		restReq = restReq.Body(bytes.NewReader(body))
	}
	requestID := proxyRequestID(r)
	for k, v := range proxyRESTHeaders(r, requestID) {
		restReq = restReq.SetHeader(k, v)
	}
	w.Header().Set("X-Request-ID", requestID)

	result := restReq.Do(ctx)
	statusCode := 0
	result.StatusCode(&statusCode)
	rawBody, err := result.Raw()
	if err != nil {
		if statusCode == 0 {
			statusCode = http.StatusBadGateway
		}
		writeError(w, statusCode, fmt.Sprintf("proxy request failed: %v", err))
		return
	}
	setProxyCORS(w, r)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cloud9-Proxied", "true")
	if statusCode > 0 {
		w.WriteHeader(statusCode)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write(rawBody)
}
