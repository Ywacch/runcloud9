package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func TestControlPlaneMetrics_AdmitCounter(t *testing.T) {
	t.Parallel()
	m := newControlPlaneMetrics()
	h := &Handler{cpMetrics: m}

	h.observeAdmitResult(context.Background(), statusProvisioning, nil)
	h.observeAdmitResult(context.Background(), "", errUserAtCapacity)

	text, err := m.gather()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `cloud9_admit_total`) {
		t.Fatalf("missing admit counter in exposition:\n%s", text)
	}
	admitted := counterValue(t, m, "cloud9_admit_total", map[string]string{"result": "admitted"})
	rejected := counterValue(t, m, "cloud9_admit_total", map[string]string{"result": "rejected"})
	if admitted != 1 {
		t.Fatalf("admitted: want 1, got %v", admitted)
	}
	if rejected != 1 {
		t.Fatalf("rejected: want 1, got %v", rejected)
	}
}

func TestControlPlaneMetrics_DoorWaitZeroOnImmediateAcquire(t *testing.T) {
	t.Parallel()
	m := newControlPlaneMetrics()
	m.observeDoorWait(0)

	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var hist *dto.Histogram
	for _, mf := range families {
		if mf.GetName() != "cloud9_door_wait_seconds" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			hist = metric.GetHistogram()
		}
	}
	if hist == nil {
		t.Fatal("missing door wait histogram")
	}
	if hist.GetSampleCount() != 1 {
		t.Fatalf("sample count: want 1, got %d", hist.GetSampleCount())
	}
	if hist.GetSampleSum() != 0 {
		t.Fatalf("sample sum: want 0, got %v", hist.GetSampleSum())
	}
}

func TestControlPlaneMetrics_ProvisionHistogram(t *testing.T) {
	t.Parallel()
	m := newControlPlaneMetrics()
	const want = 42.5
	m.observeProvision(want)

	metricFamilies, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() != "cloud9_provision_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if m.GetHistogram() != nil && m.GetHistogram().GetSampleCount() == 1 {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected one provision_seconds observation")
	}
}

func TestControlPlaneMetrics_KubeRequestCounter(t *testing.T) {
	t.Parallel()
	m := newControlPlaneMetrics()
	m.ObserveRequest("list", "pods", "metrics", 0.01)

	got := counterValue(t, m, "cloud9_k8s_requests_total", map[string]string{
		"verb":     "list",
		"resource": "pods",
		"caller":   "metrics",
	})
	if got != 1 {
		t.Fatalf("k8s requests: want 1, got %v", got)
	}
}

func TestControlPlaneMetrics_PromQueryCounter(t *testing.T) {
	t.Parallel()
	m := newControlPlaneMetrics()
	m.observePromQuery("direct", "collect", "url-shortener-small")

	got := counterValue(t, m, "cloud9_prom_queries_total", map[string]string{
		"path":     "direct",
		"caller":   "collect",
		"template": "url-shortener-small",
	})
	if got != 1 {
		t.Fatalf("prom queries: want 1, got %v", got)
	}
}

func TestControlPlaneMetrics_CollectHistogram(t *testing.T) {
	t.Parallel()
	m := newControlPlaneMetrics()
	m.observeCollectSeconds("url-shortener", 0.12)

	text, err := m.gather()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "cloud9_collect_seconds") {
		t.Fatalf("missing collect histogram in exposition:\n%s", text)
	}
}

func TestControlPlaneMetrics_GETMetrics(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "cloud9_admit_total") {
		t.Fatalf("body missing cloud9_admit_total: %s", rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "cloud9_k8s_requests_total") {
		t.Fatalf("body missing cloud9_k8s_requests_total: %s", body)
	}
	if !strings.Contains(body, "cloud9_k8s_throttle_waits_total") {
		t.Fatalf("body missing cloud9_k8s_throttle_waits_total: %s", body)
	}
}

func counterValue(t *testing.T, m *controlPlaneMetrics, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if !labelsMatch(metric.GetLabel(), labels) {
				continue
			}
			return metric.GetCounter().GetValue()
		}
	}
	t.Fatalf("counter %s labels %v not found", name, labels)
	return 0
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, p := range pairs {
		if want[p.GetName()] != p.GetValue() {
			return false
		}
	}
	return true
}

func TestRecordProvisionTimings_StampSummary(t *testing.T) {
	t.Parallel()
	s := newFakeDeploymentStore()
	ns := "ns-metrics"
	row := storeDeploymentRowWithSummary(ns, nil)
	row.CreatedAt = time.Now().Add(-2 * time.Minute)
	s.byNS[ns] = row

	h := &Handler{store: s, cpMetrics: newControlPlaneMetrics()}
	holdStart := time.Now().Add(-30 * time.Second)
	h.recordProvisionTimings(context.Background(), ns, holdStart, 5*time.Second)

	got, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if got.MetricsSummary == nil {
		t.Fatal("expected metrics_summary patch")
	}
	if _, ok := got.MetricsSummary["provision_seconds"]; !ok {
		t.Fatalf("summary: %+v", got.MetricsSummary)
	}
}
