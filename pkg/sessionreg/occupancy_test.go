package sessionreg

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ywacch/cloud9/pkg/store"
)

func int64Ptr(v int64) *int64 {
	return &v
}

func TestAdmissionOccupancy_SkipsQueued(t *testing.T) {
	fake := newFakeStore()
	queuedCPU := int64(1000)
	fake.list = []store.DeploymentRow{
		{Namespace: "ns-q", Status: "queued", AdmittedCPUm: &queuedCPU},
		{Namespace: "ns-r", Status: "running", AdmittedCPUm: int64Ptr(10)},
		{Namespace: "ns-p", Status: "provisioning", AdmittedCPUm: int64Ptr(20)},
	}
	r := newTestRegistry(fake)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	occ := r.AdmissionOccupancy()
	if occ.CPUMillicores != 30 {
		t.Fatalf("cpu sum = %d want 30", occ.CPUMillicores)
	}
}

func TestTryAdmit_Budget(t *testing.T) {
	bf := newBudgetFake()
	r := New(&store.DeploymentStore{}, fixedNow)
	r.readOverride = bf
	r.writeOverride = bf
	ctx := context.Background()

	const budget int64 = 100
	var mu sync.Mutex
	var successes atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			occ := r.AdmissionOccupancy()
			if occ.CPUMillicores+1 > budget {
				return
			}
			params := store.CreateDeploymentParams{
				Namespace:    fmt.Sprintf("ns-%d", n),
				Template:     "url-shortener",
				Status:       "provisioning",
				AdmittedCPUm: 1,
			}
			if _, err := r.CreateDeployment(ctx, params); err != nil {
				t.Errorf("CreateDeployment: %v", err)
				return
			}
			after := r.AdmissionOccupancy()
			if after.CPUMillicores > budget {
				t.Errorf("occupancy %d above budget after admit", after.CPUMillicores)
			}
			successes.Add(1)
		}(i)
	}
	wg.Wait()
	if got := successes.Load(); got != budget {
		t.Fatalf("successes = %d want %d", got, budget)
	}
}

// budgetFake tracks admitted rows for CreateDeployment in the budget stress test.
type budgetFake struct {
	fakeStore
	mu   sync.Mutex
	list []store.DeploymentRow
}

func newBudgetFake() *budgetFake {
	return &budgetFake{
		fakeStore: fakeStore{byNS: make(map[string]store.DeploymentRow)},
		list:      make([]store.DeploymentRow, 0),
	}
}

func (b *budgetFake) ListActiveForRegistry(context.Context) ([]store.DeploymentRow, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]store.DeploymentRow, len(b.list))
	copy(out, b.list)
	return out, nil
}

func (b *budgetFake) CreateDeployment(_ context.Context, params store.CreateDeploymentParams) (string, error) {
	b.fakeStore.createCalls++
	status := params.Status
	if status == "" {
		status = "provisioning"
	}
	cpu := params.AdmittedCPUm
	mem := params.AdmittedMemMi
	row := store.DeploymentRow{
		Namespace:     params.Namespace,
		Status:        status,
		Template:      params.Template,
		AdmittedCPUm:  &cpu,
		AdmittedMemMi: &mem,
	}
	b.mu.Lock()
	b.list = append(b.list, row)
	b.mu.Unlock()
	b.fakeStore.byNS[params.Namespace] = row
	return "id-" + params.Namespace, nil
}
