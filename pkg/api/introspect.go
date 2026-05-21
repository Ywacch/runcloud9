package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/introspect"
	"github.com/ywacch/cloud9/pkg/templates"
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

	action := introspectAction(r.URL.Path, namespace, component)
	if action == "" {
		writeError(w, http.StatusBadRequest, "unknown introspection action")
		return
	}

	if err := validateIntrospectAction(action, introSpec, param, r); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}

	var host string
	switch introSpec.Engine {
	case "postgres":
		svcHost, _, err := resolveProxyTarget(component, row.Design)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		host = fmt.Sprintf("%s.%s.svc.cluster.local", svcHost, namespace)
	case "redis":
		if component != "cache" {
			writeError(w, http.StatusBadRequest, "redis introspection only supports cache component")
			return
		}
		svcHost := resolveIntrospectRedisHost(row.Design)
		host = fmt.Sprintf("%s.%s.svc.cluster.local", svcHost, namespace)
	default:
		writeError(w, http.StatusBadRequest, "unsupported engine")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	switch introSpec.Engine {
	case "postgres":
		h.handlePostgresIntrospect(ctx, w, host, action, param, introSpec, r)
	case "redis":
		h.handleRedisIntrospect(ctx, w, host, action, param, introSpec, r)
	default:
		writeError(w, http.StatusBadRequest, "unsupported engine")
	}
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
	}
	return nil
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
		writeIntrospectJSON(w, map[string]any{
			"keys":   keys,
			"cursor": next,
		})
	case "key":
		kv, err := cache.GetKeyValue(ctx, key)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeIntrospectJSON(w, kv)
	default:
		writeError(w, http.StatusNotFound, "unknown redis action")
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
