package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func promNSValueJSON(values map[string]string) map[string]any {
	result := make([]map[string]any, 0, len(values))
	for ns, val := range values {
		result = append(result, map[string]any{
			"metric": map[string]string{"namespace": ns},
			"value":  []any{float64(1), val},
		})
	}
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result":     result,
		},
	}
}

func TestClusterSample_TwoNamespacesOneRefresh(t *testing.T) {
	t.Parallel()

	var promCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		promCalls.Add(1)
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "sum by (namespace)") && strings.Contains(q, "http_requests_total"):
			_ = json.NewEncoder(w).Encode(promNSValueJSON(map[string]string{
				"ns-a": "10",
				"ns-b": "20",
			}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	var promObserved, apiObserved int
	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	c.SetClusterSampleEnabled(true)
	c.SetMetricsQueryObserver(func(source string, count int) {
		switch source {
		case "prometheus":
			promObserved += count
		case "metrics_api":
			apiObserved += count
		}
	})

	rr := promRateRange()
	wantHTTP := `sum by (namespace) (sum(rate(http_requests_total` + rr + `)))`

	_, _ = c.Collect(context.Background(), "ns-a", "", TopologyFlags{})
	firstCalls := promCalls.Load()
	_, _ = c.Collect(context.Background(), "ns-b", "", TopologyFlags{})
	secondCalls := promCalls.Load()

	if secondCalls != firstCalls {
		t.Fatalf("second Collect should not hit Prometheus again in the same tick: first=%d second=%d", firstCalls, secondCalls)
	}
	if promObserved < 1 {
		t.Fatalf("expected prometheus observer callback, got %d", promObserved)
	}

	sample := c.ensureClusterSample(context.Background())
	if sample == nil {
		t.Fatal("expected cluster sample")
	}
	if got := sample.scalars[wantHTTP]["ns-a"]; got != 10 {
		t.Fatalf("ns-a rps: want 10, got %v", got)
	}
	if got := sample.scalars[wantHTTP]["ns-b"]; got != 20 {
		t.Fatalf("ns-b rps: want 20, got %v", got)
	}

	dm, err := c.Collect(context.Background(), "ns-a", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	if app, ok := dm.Components["app-server"]; !ok || app.RequestRate != 10 {
		t.Fatalf("app-server request rate: %+v", dm.Components["app-server"])
	}
}

func TestClusterSample_MissingSeriesAbsent(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	c.SetClusterSampleEnabled(true)
	dm, err := c.Collect(context.Background(), "ns-missing", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	if dm.Components["app-server"].RequestRate != 0 {
		t.Fatalf("missing series should leave request rate unset/zero, got %v", dm.Components["app-server"].RequestRate)
	}
}
