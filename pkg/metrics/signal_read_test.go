package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/metrics/hub"
	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

type hubTestQuerier struct {
	fn func(promql string) ([]hub.Sample, error)
}

func (q *hubTestQuerier) Query(_ context.Context, promql string) ([]hub.Sample, error) {
	if q.fn != nil {
		return q.fn(promql)
	}
	return nil, nil
}

func promVectorResp(ns, app, value string) map[string]any {
	labels := map[string]string{"namespace": ns}
	if app != "" {
		labels["app"] = app
	}
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result": []any{
				map[string]any{
					"metric": labels,
					"value":  []any{float64(1), value},
				},
			},
		},
	}
}

func emptyPromVector() map[string]any {
	return map[string]any{
		"status": "success",
		"data":   map[string]any{"resultType": "vector", "result": []any{}},
	}
}

func testHubWithRequestRate(t *testing.T, ns string, value float64) *hub.Hub {
	t.Helper()
	ql, err := signals.RenderCluster(signalByID(t, signals.AppRequestRate))
	if err != nil {
		t.Fatalf("RenderCluster: %v", err)
	}
	q := &hubTestQuerier{
		fn: func(promql string) ([]hub.Sample, error) {
			if promql != ql {
				return nil, nil
			}
			return []hub.Sample{{Namespace: ns, App: "app-server", Value: value}}, nil
		},
	}
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	h := hub.New(q, []signals.Signal{signalByID(t, signals.AppRequestRate)}, func() time.Time { return now }, nil, nil)
	h.RefreshOnce(context.Background())
	return h
}

func signalByID(t *testing.T, id signals.ID) signals.Signal {
	t.Helper()
	s, ok := signals.Lookup(id)
	if !ok {
		t.Fatalf("signal %q", id)
	}
	return s
}

func TestSig_HubAndDirectMatch(t *testing.T) {
	t.Parallel()

	ns := "ns-a"
	want := 3.0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(promVectorResp(ns, "app-server", "3"))
	}))
	t.Cleanup(srv.Close)

	cDirect := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	cHub := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	cHub.SetMetricsHub(testHubWithRequestRate(t, ns, want))

	ctx := context.Background()
	vDirect, okDirect := cDirect.sig(ctx, ns, signals.AppRequestRate)
	vHub, okHub := cHub.sig(ctx, ns, signals.AppRequestRate)
	if !okDirect || !okHub {
		t.Fatalf("ok: direct=%v hub=%v", okDirect, okHub)
	}
	if vDirect != want || vHub != want || vDirect != vHub {
		t.Fatalf("direct=%v hub=%v want %v", vDirect, vHub, want)
	}
}

func TestSig_MissingUnknown(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(emptyPromVector())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	_, ok := c.sig(context.Background(), "ns-a", signals.AppLatencyP50)
	if ok {
		t.Fatal("expected unknown missing to return ok=false")
	}
}

func TestSig_MissingZero(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(emptyPromVector())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	v, ok := c.sig(context.Background(), "ns-a", signals.AppRequestRate)
	if !ok || v != 0 {
		t.Fatalf("got %v ok=%v", v, ok)
	}
}
