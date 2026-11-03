package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ywacch/cloud9/pkg/introspect"
	"github.com/ywacch/cloud9/pkg/templates"
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

func TestSocialFeedCacheAllowsWorkQueueStream(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("social-feed")
	if tmpl == nil {
		t.Fatal("social-feed template missing")
	}
	cache := tmpl.Introspection["cache"]
	mq := tmpl.Introspection["mq"]
	cache.KeyPatterns = introspect.AppendQueueStreamKeys(cache.KeyPatterns, mq.Queues)
	keysReq := httptest.NewRequest(http.MethodGet, "/keys?pattern=fanout", nil)
	if err := validateIntrospectAction("keys", cache, "", keysReq); err != nil {
		t.Fatal(err)
	}
	if err := validateIntrospectAction("key", cache, "fanout", httptest.NewRequest(http.MethodGet, "/key/fanout", nil)); err != nil {
		t.Fatal(err)
	}
	timelineReq := httptest.NewRequest(http.MethodGet, "/keys?pattern=timeline:*", nil)
	if err := validateIntrospectAction("keys", cache, "", timelineReq); err != nil {
		t.Fatal(err)
	}
}

func TestValidateIntrospectAction_StatsNeedsNoPattern(t *testing.T) {
	t.Parallel()
	spec := introspect.ComponentSpec{
		Engine:      "redis",
		KeyPatterns: []string{"timeline:*"},
	}
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	if err := validateIntrospectAction("stats", spec, "", req); err != nil {
		t.Fatalf("stats: %v", err)
	}
}

func TestMemcachedKeyPatterns_EmptyQueryUsesWhitelist(t *testing.T) {
	t.Parallel()
	allowed := []string{"product:*", "cart:*", "catalog", "product-pool"}
	got := memcachedKeyPatterns("", allowed)
	if len(got) != len(allowed) {
		t.Fatalf("len = %d", len(got))
	}
	for i := range allowed {
		if got[i] != allowed[i] {
			t.Fatalf("pattern[%d] = %q", i, got[i])
		}
	}
	allowed[0] = "changed"
	if got[0] != "product:*" {
		t.Fatal("result aliases the whitelist slice")
	}
	one := memcachedKeyPatterns(" catalog ", allowed)
	if len(one) != 1 || one[0] != "catalog" {
		t.Fatalf("query pattern = %#v", one)
	}
}

func TestValidateIntrospectAction_MemcachedEmptyPatternAllowed(t *testing.T) {
	t.Parallel()
	spec := introspect.ComponentSpec{
		Engine:      "memcached",
		KeyPatterns: []string{"product:*", "catalog"},
	}
	req := httptest.NewRequest(http.MethodGet, "/keys", nil)
	if err := validateIntrospectAction("keys", spec, "", req); err != nil {
		t.Fatalf("empty pattern: %v", err)
	}
}

func TestEcommerceCacheMemcachedKeepsTemplateKeyPatterns(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("ecommerce")
	if tmpl == nil {
		t.Fatal("ecommerce template missing")
	}
	spec := tmpl.Introspection["cache"]
	spec.Engine = "memcached"
	if len(spec.KeyPatterns) == 0 {
		t.Fatal("expected cache keyPatterns on ecommerce template")
	}
	req := httptest.NewRequest(http.MethodGet, "/keys", nil)
	if err := validateIntrospectAction("keys", spec, "", req); err != nil {
		t.Fatalf("keys list without query: %v", err)
	}
}

func TestValidateIntrospectAction_KeysStarForbiddenWhenNotWhitelisted(t *testing.T) {
	t.Parallel()
	spec := introspect.ComponentSpec{
		Engine:      "redis",
		KeyPatterns: []string{"timeline:*"},
	}
	req := httptest.NewRequest(http.MethodGet, "/keys?pattern=*", nil)
	if err := validateIntrospectAction("keys", spec, "", req); err == nil {
		t.Fatal("expected error for pattern *")
	}
}

func TestResolveIntrospectHost_RabbitMQ(t *testing.T) {
	t.Parallel()
	host, err := resolveIntrospectHost(context.Background(), "rabbitmq", "mq", "ns1", "primary", "", nil, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if host != "mq.ns1.svc.cluster.local" {
		t.Fatalf("host=%q", host)
	}
	if _, err := resolveIntrospectHost(context.Background(), "rabbitmq", "cache", "ns1", "primary", "", nil, nil); err == nil {
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
			host, err := resolveIntrospectHost(context.Background(), tc.engine, tc.component, tc.namespace, tc.target, "", tc.design, nil)
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

func TestResolveIntrospectHost_ReplicaLayerID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	design := map[string]any{
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
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "postgres-replica-roll",
			Namespace: "ns-layer",
			Labels:    map[string]string{"app": "postgres-replica"},
		},
		Status: corev1.PodStatus{PodIP: "10.42.0.15"},
	}
	clientset := fake.NewSimpleClientset(pod)

	host, err := resolveIntrospectHost(ctx, "postgres", "db", "ns-layer", "replica", "postgres-replica-roll", design, clientset)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if host != "10.42.0.15" {
		t.Fatalf("host=%q want pod IP", host)
	}

	if _, err := resolveIntrospectHost(ctx, "postgres", "db", "ns-layer", "primary", "postgres-replica-roll", design, clientset); err == nil {
		t.Fatal("expected error when layerId is set on primary target")
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
