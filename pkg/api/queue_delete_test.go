package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestHandleDeleteDeployment_QueuedSkipsTeardown(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")

	var teardownCalls atomic.Int32
	s := newFakeDeploymentStore()
	ns := "ns-queued-del"
	s.byNS[ns] = store.DeploymentRow{
		ID: "1", UserID: "dev-user", Namespace: ns, Template: "url-shortener",
		Status: "queued", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}
	s.active = append(s.active, store.ActiveDeploymentRow{
		ID: "1", Namespace: ns, Template: "url-shortener", Status: "queued",
		CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	})

	deployers := map[string]deployer.ComponentDeployer{
		"db": &fakeBlockingDeployer{
			teardownFn: func(ctx context.Context, namespace string) error {
				teardownCalls.Add(1)
				return nil
			},
		},
	}
	h := NewHandler(deployers, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodDelete, "/deployments/"+ns, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 body=%s", rec.Code, rec.Body.String())
	}
	if teardownCalls.Load() != 0 {
		t.Fatalf("teardown called %d times, want 0", teardownCalls.Load())
	}
	row, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "terminated" {
		t.Fatalf("status = %q, want terminated", row.Status)
	}
}
