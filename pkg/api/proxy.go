package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Max-Age", "86400")
	_ = r // reserved for future origin checks
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
		switch strategy {
		case "write-through", "write-behind", "read-through":
			return 8081
		default:
			return 6379
		}
	}
	return 6379
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
		return "mq", 15672, nil
	case "cache":
		return "cache", cacheServiceHTTPPort(design), nil
	case "worker":
		return "", 0, fmt.Errorf("worker has no HTTP ClusterIP service in this stack")
	default:
		return "", 0, fmt.Errorf("unknown component %q", component)
	}
}

func buildProxyURL(namespace, host string, port int, proxyPath string) string {
	base := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", host, namespace, port)
	proxyPath = strings.TrimPrefix(proxyPath, "/")
	if proxyPath == "" {
		return base
	}
	return base + "/" + proxyPath
}

// handleProxy forwards HTTP to a component Service inside the deployment namespace.
// Routes: /deployments/{id}/proxy/{component} and /deployments/{id}/proxy/{component}/{path...}
func (h *Handler) handleProxy(w http.ResponseWriter, r *http.Request) {
	setProxyCORS(w, r)
	if r.Method == http.MethodOptions {
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

	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "deployment store unavailable")
		return
	}

	row, err := h.store.GetDeployment(r.Context(), namespace)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "deployment not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to resolve deployment")
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

	targetURL := buildProxyURL(namespace, svcHost, svcPort, proxyPath)

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var body io.Reader
	if r.Body != nil {
		body = http.MaxBytesReader(w, r.Body, 1<<20)
	}

	proxyReq, err := http.NewRequestWithContext(ctx, r.Method, targetURL, body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create proxy request")
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		proxyReq.Header.Set("Content-Type", ct)
	}
	if accept := r.Header.Get("Accept"); accept != "" {
		proxyReq.Header.Set("Accept", accept)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(proxyReq)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("proxy request failed: %v", err))
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		if _, skip := hopByHopResponseHeaders[http.CanonicalHeaderKey(k)]; skip {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	setProxyCORS(w, r)
	w.Header().Set("X-Cloud9-Proxied", "true")
	w.WriteHeader(resp.StatusCode)

	_, _ = io.Copy(w, io.LimitReader(resp.Body, 5<<20))
}
