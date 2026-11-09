package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics/sidework"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestTeardownIntent_ExpiresEarlierThanEnqueue(t *testing.T) {
	enqueued := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	expires := enqueued.Add(-2 * time.Minute)
	got := teardownIntent(enqueued, expires)
	if !got.Equal(expires) {
		t.Fatalf("intent = %s, want expires %s", got, expires)
	}
}

func TestTeardownIntent_DeleteBeforeExpiry(t *testing.T) {
	enqueued := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	expires := enqueued.Add(5 * time.Minute)
	got := teardownIntent(enqueued, expires)
	if !got.Equal(enqueued) {
		t.Fatalf("intent = %s, want enqueue %s", got, enqueued)
	}
}

func TestTeardownDiagnostics_SideWorkPoolTimesOut(t *testing.T) {
	t.Parallel()
	pool := sidework.New(1, time.Now)
	release, ok := pool.Acquire(context.Background(), 0)
	if !ok {
		t.Fatal("expected first acquire")
	}
	_, ok = pool.Acquire(context.Background(), 20*time.Millisecond)
	if ok {
		t.Fatal("expected the wait to end with no free slot")
	}
	release()
	if _, ok = pool.Acquire(context.Background(), 20*time.Millisecond); !ok {
		t.Fatal("expected a free slot after release")
	}
}

func TestClaimStatus_TerminatingKeepsAdmissionWeight(t *testing.T) {
	s := newFakeDeploymentStore()
	ns := "ns-weight"
	s.byNS[ns] = store.DeploymentRow{Namespace: ns, Status: statusRunning}
	s.active = []store.ActiveDeploymentRow{{Namespace: ns, Status: statusRunning}}
	s.admittedByNS[ns] = store.AdmissionOccupancy{CPUMillicores: 100, MemoryMi: 10}

	ok, err := s.ClaimStatus(context.Background(), ns, []string{statusRunning}, statusTerminating)
	if err != nil || !ok {
		t.Fatalf("claim terminating: ok=%v err=%v", ok, err)
	}
	occ, err := s.SumActiveAdmissionWeights(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if occ.CPUMillicores != 100 || occ.MemoryMi != 10 {
		t.Fatalf("occupancy while terminating = %+v", occ)
	}

	ok, err = s.ClaimStatus(context.Background(), ns, []string{statusProvisioning}, statusRunning)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("provisioning claim overwrote terminating")
	}
	row, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != statusTerminating {
		t.Fatalf("status = %s", row.Status)
	}

	ok, err = s.ClaimStatus(context.Background(), ns, []string{statusTerminating}, statusTerminated)
	if err != nil || !ok {
		t.Fatalf("claim terminated: ok=%v err=%v", ok, err)
	}
	occ, err = s.SumActiveAdmissionWeights(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if occ.CPUMillicores != 0 || occ.MemoryMi != 0 {
		t.Fatalf("occupancy after terminated = %+v", occ)
	}
}

func TestHandleDeleteDeployment_ReturnsBeforeNamespaceDelete(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")

	ns := "ns-async-del"
	s := newFakeDeploymentStore()
	s.byNS[ns] = store.DeploymentRow{
		ID: "1", UserID: "dev-user", Namespace: ns, Template: "url-shortener",
		Status: statusRunning, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}
	s.active = []store.ActiveDeploymentRow{{
		ID: "1", Namespace: ns, Template: "url-shortener", Status: statusRunning,
		CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}}

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	h := NewHandler(map[string]deployer.ComponentDeployer{
		"db": &fakeBlockingDeployer{
			teardownFn: func(ctx context.Context, namespace string) error {
				calls.Add(1)
				once.Do(func() { close(started) })
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			},
		},
	}, s, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := httptest.NewRequest(http.MethodDelete, "/deployments/"+ns, nil)
		mux.ServeHTTP(rec, req)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DELETE blocked on namespace teardown")
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 body=%s", rec.Code, rec.Body.String())
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("teardown worker did not start")
	}
	row, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != statusTerminating {
		t.Fatalf("status before namespace delete = %s, want terminating", row.Status)
	}

	again := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/deployments/"+ns, nil)
	mux.ServeHTTP(again, req)
	if again.Code != http.StatusNoContent {
		t.Fatalf("second DELETE status = %d", again.Code)
	}

	close(release)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		row, err = s.GetDeployment(context.Background(), ns)
		if err != nil {
			t.Fatal(err)
		}
		if row.Status == statusTerminated {
			if calls.Load() != 1 {
				t.Fatalf("teardown calls = %d, want 1", calls.Load())
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("status = %s, want terminated", row.Status)
}

func TestRecoverTerminating_EnqueuesTeardown(t *testing.T) {
	ns := "ns-recover"
	s := newFakeDeploymentStore()
	s.byNS[ns] = store.DeploymentRow{
		ID: "1", UserID: "dev-user", Namespace: ns, Status: statusTerminating,
		CreatedAt: time.Now().Add(-10 * time.Minute), ExpiresAt: time.Now().Add(-time.Minute),
	}
	h := NewHandler(map[string]deployer.ComponentDeployer{
		"db": &fakeBlockingDeployer{},
	}, s, nil, nil, nil, nil, nil, nil, nil)

	h.recoverTerminating(context.Background())

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		row, err := s.GetDeployment(context.Background(), ns)
		if err != nil {
			t.Fatal(err)
		}
		if row.Status == statusTerminated {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("terminating row was not torn down after recover")
}
