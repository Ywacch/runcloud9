package metrics

import (
	"context"
	"encoding/json"
	"fmt"
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
		case strings.Contains(q, "sum by (namespace)") && strings.Contains(q, "http_requests_total") && !strings.Contains(q, "status="):
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
	wantHTTP := `sum by (namespace) (rate(http_requests_total` + rr + `))`
	sessionQL := fmt.Sprintf(`sum(rate(http_requests_total{namespace=%q}%s))`, "ns-a", rr)
	gotKey, okKey := promQLToClusterScalarKey(sessionQL)
	if !okKey || gotKey != wantHTTP {
		t.Fatalf("session query key: ok=%v got %q want %q", okKey, gotKey, wantHTTP)
	}

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

func TestIngestPromVector_SkipsUnlabeledNamespace(t *testing.T) {
	t.Parallel()

	rr := promRateRange()
	bucketQL := `sum by (namespace, le) (rate(http_request_duration_seconds_bucket` + rr + `))`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if q == bucketQL {
			_ = json.NewEncoder(w).Encode(promNSBucketJSON("ns-p99", map[string]string{
				"0.05": "10",
				"0.1":  "50",
				"0.25": "100",
				"+Inf": "100",
			}))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	c.SetClusterSampleEnabled(true)
	nsSel := fmt.Sprintf(`namespace=%q`, "ns-p99")
	inner := fmt.Sprintf(`sum(rate(http_request_duration_seconds_bucket{%s}%s)) by (le)`, nsSel, rr)
	p99QL := fmt.Sprintf(`histogram_quantile(0.99, %s)`, inner)
	p99, err := c.query(context.Background(), p99QL)
	if err != nil {
		t.Fatal(err)
	}
	if p99 <= 0 {
		t.Fatalf("expected p99 from cluster buckets, got %v", p99)
	}
}

func promNSBucketJSON(ns string, leToVal map[string]string) map[string]any {
	result := make([]map[string]any, 0, len(leToVal))
	for le, val := range leToVal {
		result = append(result, map[string]any{
			"metric": map[string]string{"namespace": ns, "le": le},
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

func TestClusterSampleEnabled(t *testing.T) {
	t.Parallel()
	const url = "http://prom.example:9090"
	tests := []struct {
		env  string
		prom string
		want bool
	}{
		{"", url, false},
		{"true", url, true},
		{"TRUE", url, true},
		{"1", url, true},
		{"false", url, false},
		{"0", url, false},
		{"yes", url, false},
		{"true", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.env+"_"+tc.prom, func(t *testing.T) {
			got := ClusterSampleEnabled(tc.prom, tc.env)
			if got != tc.want {
				t.Fatalf("ClusterSampleEnabled(%q, %q) = %v, want %v", tc.prom, tc.env, got, tc.want)
			}
		})
	}
}

func TestQueryScalar_SampleOffHitsPrometheus(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result": []map[string]any{
					{"metric": map[string]string{}, "value": []any{float64(1), "4"}},
				},
			},
		})
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	val, present, err := c.queryScalar(context.Background(), `sum(up)`)
	if err != nil {
		t.Fatal(err)
	}
	if !present || val != 4 {
		t.Fatalf("queryScalar: present=%v val=%v, want present true val 4", present, val)
	}
}
