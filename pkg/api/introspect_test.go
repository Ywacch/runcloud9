package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ywacch/cloud9/pkg/introspect"
)

func TestIntrospectRequiresAuth(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "test-secret")
	t.Setenv("SUPABASE_URL", "https://example.supabase.co")
	t.Setenv("ENV", "production")

	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/deployments/test-ns/introspect/db/tables", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestIntrospectRejectsUnwhitelistedTable(t *testing.T) {
	t.Parallel()
	spec := introspect.ComponentSpec{
		Engine: "postgres",
		Tables: []string{"urls"},
	}
	err := validateIntrospectAction("rows", spec, "pg_catalog", httptest.NewRequest(http.MethodGet, "/", nil))
	if err == nil {
		t.Fatal("expected error for unwhitelisted table")
	}
}

func TestIntrospectRejectsUnwhitelistedQueue(t *testing.T) {
	t.Parallel()
	spec := introspect.ComponentSpec{
		Engine: "rabbitmq",
		Queues: []string{"fanout"},
	}
	err := validateIntrospectAction("peek", spec, "secret", httptest.NewRequest(http.MethodGet, "/", nil))
	if err == nil {
		t.Fatal("expected error for unwhitelisted queue")
	}
}

func TestResolveIntrospectHost_RabbitMQ(t *testing.T) {
	t.Parallel()
	host, err := resolveIntrospectHost("rabbitmq", "mq", "ns1", "primary", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if host != "mq.ns1.svc.cluster.local" {
		t.Fatalf("host=%q", host)
	}
	if _, err := resolveIntrospectHost("rabbitmq", "cache", "ns1", "primary", nil); err == nil {
		t.Fatal("expected error for non-mq component")
	}
}

func TestParseIntrospectTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw     string
		want    string
		wantErr bool
	}{
		{raw: "", want: "primary"},
		{raw: "primary", want: "primary"},
		{raw: "replica", want: "replica"},
		{raw: "standby", wantErr: true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.raw, func(t *testing.T) {
			t.Parallel()
			got, err := parseIntrospectTarget(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestResolveIntrospectHost_TargetReplica(t *testing.T) {
	t.Parallel()

	designWithReplica := map[string]any{
		"nodes": []any{
			map[string]any{
				"id":   "db1",
				"type": "db",
				"config": map[string]any{
					"readReplicas": 1,
				},
			},
		},
	}
	designNoReplica := map[string]any{
		"nodes": []any{
			map[string]any{
				"id":   "db1",
				"type": "db",
				"config": map[string]any{
					"readReplicas": 0,
				},
			},
		},
	}
	designCache := map[string]any{
		"nodes": []any{
			map[string]any{
				"id":   "cache1",
				"type": "cache",
				"config": map[string]any{
					"strategy": "cache-aside",
				},
			},
		},
	}

	tests := []struct {
		name      string
		engine    string
		component string
		namespace string
		target    string
		design    map[string]any
		wantHost  string
		wantErr   bool
	}{
		{
			name:      "postgres db with readReplicas routes to replica service",
			engine:    "postgres",
			component: "db",
			namespace: "ns1",
			target:    "replica",
			design:    designWithReplica,
			wantHost:  "db-replica.ns1.svc.cluster.local",
		},
		{
			name:      "readReplicas zero rejects replica target",
			engine:    "postgres",
			component: "db",
			namespace: "ns1",
			target:    "replica",
			design:    designNoReplica,
			wantErr:   true,
		},
		{
			name:      "redis cache rejects replica target",
			engine:    "redis",
			component: "cache",
			namespace: "ns1",
			target:    "replica",
			design:    designCache,
			wantErr:   true,
		},
		{
			name:      "default target resolves to primary db",
			engine:    "postgres",
			component: "db",
			namespace: "ns1",
			target:    "primary",
			design:    designWithReplica,
			wantHost:  "db.ns1.svc.cluster.local",
		},
		{
			name:      "omitted target resolves to primary db",
			engine:    "postgres",
			component: "db",
			namespace: "ns1",
			target:    "",
			design:    designWithReplica,
			wantHost:  "db.ns1.svc.cluster.local",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			host, err := resolveIntrospectHost(tc.engine, tc.component, tc.namespace, tc.target, tc.design)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if host != tc.wantHost {
				t.Fatalf("host=%q want %q", host, tc.wantHost)
			}
		})
	}
}

func TestFilterTables_ExcludesPlatformTables(t *testing.T) {
	t.Parallel()
	tables := []introspect.TableInfo{
		{Name: "urls", RowCount: 10},
		{Name: "_c9_probe", RowCount: 1},
	}
	got := filterTables(tables, []string{"urls", "_c9_probe"})
	if len(got) != 1 || got[0].Name != "urls" {
		t.Fatalf("filterTables: %+v", got)
	}
}
