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
	host, err := resolveIntrospectHost("rabbitmq", "mq", "ns1", nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if host != "mq.ns1.svc.cluster.local" {
		t.Fatalf("host=%q", host)
	}
	if _, err := resolveIntrospectHost("rabbitmq", "cache", "ns1", nil); err == nil {
		t.Fatal("expected error for non-mq component")
	}
}
