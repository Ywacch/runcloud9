package hub

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

type fakeQuerier struct {
	calls atomic.Int32
	fn    func(promql string) ([]Sample, error)
}

func (f *fakeQuerier) Query(_ context.Context, promql string) ([]Sample, error) {
	f.calls.Add(1)
	if f.fn != nil {
		return f.fn(promql)
	}
	return nil, nil
}

func TestHub_TwoNamespaces(t *testing.T) {
	t.Parallel()

	ql, err := signals.RenderCluster(signalByID(t, signals.AppRequestRate))
	if err != nil {
		t.Fatalf("RenderCluster: %v", err)
	}
	q := &fakeQuerier{
		fn: func(promql string) ([]Sample, error) {
			if promql != ql {
				return nil, nil
			}
			return []Sample{
				{Namespace: "ns-a", App: "app-server", Value: 10},
				{Namespace: "ns-b", App: "app-server", Value: 20},
			}, nil
		},
	}
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	h := New(q, []signals.Signal{signalByID(t, signals.AppRequestRate)}, func() time.Time { return now }, nil, nil)
	h.refresh(context.Background())
	callsAfterRefresh := q.calls.Load()

	snap := h.Snapshot()
	for _, ns := range []string{"ns-a", "ns-b"} {
		m, ok, _ := snap.ByApp(ns, signals.AppRequestRate)
		if !ok || m["app-server"] <= 0 {
			t.Fatalf("namespace %q: %+v ok=%v", ns, m, ok)
		}
	}
	_ = h.Snapshot()
	if q.calls.Load() != callsAfterRefresh {
		t.Fatalf("Snapshot() must not query prometheus again")
	}
}

func TestHub_DropsUnlabeled(t *testing.T) {
	t.Parallel()

	var unlabeled int
	obs := &testObserver{unlabeled: &unlabeled}
	q := &fakeQuerier{
		fn: func(string) ([]Sample, error) {
			return []Sample{
				{Namespace: "", App: "app-server", Value: 1},
				{Namespace: "ns-a", App: "app-server", Value: 5},
			}, nil
		},
	}
	h := New(q, []signals.Signal{signalByID(t, signals.AppRequestRate)}, time.Now, obs, nil)
	h.refresh(context.Background())
	if unlabeled != 1 {
		t.Fatalf("unlabeled counter: want 1 got %d", unlabeled)
	}
	m, ok, _ := h.Snapshot().ByApp("ns-a", signals.AppRequestRate)
	if !ok || m["app-server"] != 5 {
		t.Fatalf("ns-a: %+v ok=%v", m, ok)
	}
	_, ok2, _ := h.Snapshot().ByApp("", signals.AppRequestRate)
	if ok2 {
		t.Fatal("empty namespace should not be present")
	}
}

func TestHub_KeepsLastGoodFor15s(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	fail := false
	q := &fakeQuerier{
		fn: func(string) ([]Sample, error) {
			if fail {
				return nil, errTestQuery
			}
			return []Sample{{Namespace: "ns-a", App: "app-server", Value: 3}}, nil
		},
	}
	clock := func() time.Time { return now }
	h := New(q, []signals.Signal{signalByID(t, signals.AppRequestRate)}, clock, nil, nil)
	h.refresh(context.Background())

	fail = true
	now = now.Add(5 * time.Second)
	h.refresh(context.Background())
	_, ok, stale := h.Snapshot().ByApp("ns-a", signals.AppRequestRate)
	if !ok || !stale {
		t.Fatalf("after failed refresh: ok=%v stale=%v", ok, stale)
	}

	now = now.Add(16 * time.Second)
	h.refresh(context.Background())
	_, ok, stale = h.Snapshot().ByApp("ns-a", signals.AppRequestRate)
	if ok || stale {
		t.Fatalf("after stale grace: ok=%v stale=%v", ok, stale)
	}
}

var errTestQuery = &testQueryError{}

type testQueryError struct{}

func (e *testQueryError) Error() string { return "test query failure" }

type testObserver struct {
	unlabeled *int
}

func (o *testObserver) ObservePromQuery(string, string, string) {}
func (o *testObserver) ObserveRefresh(float64)                  {}
func (o *testObserver) IncSignalError(string)                   {}
func (o *testObserver) IncSignalAbsent(string)                  {}
func (o *testObserver) IncUnlabeledSeries(string) {
	if o != nil && o.unlabeled != nil {
		*o.unlabeled++
	}
}

func signalByID(t *testing.T, id signals.ID) signals.Signal {
	t.Helper()
	for _, s := range signals.Catalog() {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("signal %q not in catalog", id)
	return signals.Signal{}
}
