package sessionreg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

var fixedNow = func() time.Time {
	return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
}

type fakeStore struct {
	getCalls     int
	list         []store.DeploymentRow
	byNS         map[string]store.DeploymentRow
	updateCalls  int
	lastUpdate   string
	lastUpdateSt string
	createCalls  int
}

func newFakeStore() *fakeStore {
	return &fakeStore{byNS: make(map[string]store.DeploymentRow)}
}

func (f *fakeStore) GetDeployment(_ context.Context, namespace string) (store.DeploymentRow, error) {
	f.getCalls++
	row, ok := f.byNS[namespace]
	if !ok {
		return store.DeploymentRow{}, fmt.Errorf("store.GetDeployment: not found")
	}
	return row, nil
}

func (f *fakeStore) ListActiveForRegistry(context.Context) ([]store.DeploymentRow, error) {
	if f.list != nil {
		return append([]store.DeploymentRow(nil), f.list...), nil
	}
	out := make([]store.DeploymentRow, 0, len(f.byNS))
	for _, row := range f.byNS {
		if isActiveStatus(row.Status) {
			out = append(out, row)
		}
	}
	return out, nil
}

func (f *fakeStore) CreateDeployment(_ context.Context, params store.CreateDeploymentParams) (string, error) {
	f.createCalls++
	status := params.Status
	if status == "" {
		status = "provisioning"
	}
	id := "id-" + params.Namespace
	f.byNS[params.Namespace] = deploymentRowFromCreate(params, id, status, fixedNow())
	return id, nil
}

func (f *fakeStore) UpdateStatus(_ context.Context, namespace, status string) error {
	f.updateCalls++
	f.lastUpdate = namespace
	f.lastUpdateSt = status
	row, ok := f.byNS[namespace]
	if !ok {
		return fmt.Errorf("store.UpdateStatus: missing row")
	}
	row.Status = status
	f.byNS[namespace] = row
	return nil
}

func newTestRegistry(fake *fakeStore) *Registry {
	r := New(&store.DeploymentStore{}, fixedNow)
	r.readOverride = fake
	r.writeOverride = fake
	return r
}

func TestGet_HitSkipsInner(t *testing.T) {
	fake := newFakeStore()
	fake.list = []store.DeploymentRow{{
		Namespace: "ns-a",
		Status:    "running",
		UserID:    "u1",
	}}
	r := newTestRegistry(fake)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	fake.getCalls = 0
	if _, err := r.GetDeployment(context.Background(), "ns-a"); err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if fake.getCalls != 0 {
		t.Fatalf("inner get calls = %d want 0", fake.getCalls)
	}
}

func TestGet_MissCallsInner(t *testing.T) {
	fake := newFakeStore()
	r := newTestRegistry(fake)
	if _, err := r.GetDeployment(context.Background(), "missing"); err == nil {
		t.Fatal("expected not found")
	}
	if fake.getCalls != 1 {
		t.Fatalf("inner get calls = %d want 1", fake.getCalls)
	}
}

func TestGet_TerminatedIsInactive(t *testing.T) {
	fake := newFakeStore()
	fake.byNS["ns-dead"] = store.DeploymentRow{Namespace: "ns-dead", Status: "terminated"}
	r := newTestRegistry(fake)
	if _, err := r.GetDeployment(context.Background(), "ns-dead"); err == nil {
		t.Fatal("expected not found")
	}
	if fake.getCalls != 1 {
		t.Fatalf("first get calls = %d want 1", fake.getCalls)
	}
	fake.getCalls = 0
	if _, err := r.GetDeployment(context.Background(), "ns-dead"); err == nil {
		t.Fatal("expected not found")
	}
	if fake.getCalls != 0 {
		t.Fatalf("second get calls = %d want 0", fake.getCalls)
	}
}

func TestCreate_ThenGetHitsMemory(t *testing.T) {
	fake := newFakeStore()
	r := newTestRegistry(fake)
	params := store.CreateDeploymentParams{
		Namespace: "ns-new",
		Template:  "url-shortener",
		UserID:    "u1",
		ExpiresAt: fixedNow().Add(time.Hour),
	}
	if _, err := r.CreateDeployment(context.Background(), params); err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}
	fake.getCalls = 0
	row, err := r.GetDeployment(context.Background(), "ns-new")
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if row.Namespace != "ns-new" {
		t.Fatalf("namespace = %q", row.Namespace)
	}
	if fake.getCalls != 0 {
		t.Fatalf("inner get calls = %d want 0", fake.getCalls)
	}
}

func TestLoad_DropsRowRemovedUpstream(t *testing.T) {
	fake := newFakeStore()
	fake.list = []store.DeploymentRow{{Namespace: "ns-a", Status: "running", UserID: "u1"}}
	r := newTestRegistry(fake)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	fake.list = nil
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	fake.byNS["ns-a"] = store.DeploymentRow{Namespace: "ns-a", Status: "running", UserID: "u1"}
	fake.getCalls = 0
	if _, err := r.GetDeployment(context.Background(), "ns-a"); err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if fake.getCalls != 1 {
		t.Fatalf("inner get calls = %d want 1", fake.getCalls)
	}
}

func TestListActive_FiltersUser(t *testing.T) {
	fake := newFakeStore()
	fake.list = []store.DeploymentRow{
		{Namespace: "ns-a", Status: "running", UserID: "u1"},
		{Namespace: "ns-b", Status: "provisioning", UserID: "u2"},
	}
	r := newTestRegistry(fake)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	all, err := r.ListActiveDeployments(context.Background(), "")
	if err != nil {
		t.Fatalf("ListActiveDeployments: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("all len = %d want 2", len(all))
	}
	u1, err := r.ListActiveDeployments(context.Background(), "u1")
	if err != nil {
		t.Fatalf("ListActiveDeployments u1: %v", err)
	}
	if len(u1) != 1 || u1[0].Namespace != "ns-a" {
		t.Fatalf("u1 rows = %+v", u1)
	}
}

func TestUpdateStatus_RemovesTerminated(t *testing.T) {
	fake := newFakeStore()
	row := store.DeploymentRow{Namespace: "ns-a", Status: "running", UserID: "u1"}
	fake.list = []store.DeploymentRow{row}
	fake.byNS[row.Namespace] = row
	r := newTestRegistry(fake)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := r.UpdateStatus(context.Background(), "ns-a", "terminated"); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if _, ok := r.activeRow("ns-a"); ok {
		t.Fatal("row still active in cache")
	}
	if !r.isInactive("ns-a") {
		t.Fatal("expected inactive")
	}
	fake.getCalls = 0
	if _, err := r.GetDeployment(context.Background(), "ns-a"); err == nil {
		t.Fatal("expected not found")
	}
	if fake.getCalls != 0 {
		t.Fatalf("inner get calls = %d want 0 after inactive", fake.getCalls)
	}
}
