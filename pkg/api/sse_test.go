package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/logs"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestWantsSSE(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		accept  string
		upgrade string
		want    bool
	}{
		{"event stream", "text/event-stream", "", true},
		{"ws upgrade wins", "text/event-stream", "websocket", false},
		{"empty accept", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/deployments/ns/metrics", nil)
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			if tc.upgrade != "" {
				req.Header.Set("Upgrade", tc.upgrade)
			}
			if got := wantsSSE(req); got != tc.want {
				t.Fatalf("wantsSSE() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHandleMetricsStream_SSEWritesFrameAndPing(t *testing.T) {
	t.Parallel()

	const ns = "ns-sse-metrics"
	ts := time.Date(2026, 11, 16, 11, 0, 0, 123456789, time.UTC)
	collector := metrics.NewMetricsCollector("", nil, nil, nil, nil)
	collector.SetPublisherCollectHookForTest(func(_ context.Context, namespace, _ string, _ metrics.TopologyFlags) (*metrics.DeploymentMetrics, error) {
		return &metrics.DeploymentMetrics{
			Timestamp:  ts,
			Namespace:  namespace,
			Components: map[string]metrics.ComponentMetrics{},
		}, nil
	})
	st := newFakeDeploymentStore()
	row := store.DeploymentRow{Namespace: ns, Template: "url-shortener"}
	st.byNS[ns] = row
	h := &Handler{store: st, metricsCollector: collector}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/deployments/"+ns+"/metrics", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	rec := newSSETestResponse()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.streamMetricsSSE(rec, req, &row, metricsSSEOptions{
			emitInterval: 40 * time.Millisecond,
			pingInterval: 25 * time.Millisecond,
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	var body string
	for time.Now().Before(deadline) {
		body = rec.bodyString()
		if strings.Contains(body, "id:") && strings.Contains(body, "data:") && strings.Contains(body, ": ping") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if !strings.Contains(body, "id:") || !strings.Contains(body, "data:") {
		t.Fatalf("missing sse frame:\n%s", body)
	}
	if !strings.Contains(body, ": ping") {
		t.Fatalf("missing ping comment:\n%s", body)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q", got)
	}
}

func TestHandleMetricsStream_LastEventIDSendsNewerSnapshot(t *testing.T) {
	t.Parallel()

	const ns = "ns-sse-resume"
	ts := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	collector := metrics.NewMetricsCollector("", nil, nil, nil, nil)
	collector.SetPublisherCollectHookForTest(func(_ context.Context, namespace, _ string, _ metrics.TopologyFlags) (*metrics.DeploymentMetrics, error) {
		return &metrics.DeploymentMetrics{
			Timestamp:  ts,
			Namespace:  namespace,
			Components: map[string]metrics.ComponentMetrics{},
		}, nil
	})
	collector.EnsurePublisher(ns, "url-shortener", metrics.TopologyFlags{})
	waitCollectPendingCleared(t, collector, ns)

	st := newFakeDeploymentStore()
	row := store.DeploymentRow{Namespace: ns, Template: "url-shortener"}
	st.byNS[ns] = row
	h := &Handler{store: st, metricsCollector: collector}

	t.Run("olderLastEventID", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Last-Event-ID", ts.Add(-time.Second).Format(time.RFC3339Nano))
		rec := newSSETestResponse()

		done := make(chan struct{})
		go func() {
			defer close(done)
			h.streamMetricsSSE(rec, req, &row, metricsSSEOptions{emitInterval: time.Hour, pingInterval: time.Hour})
		}()

		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if strings.Contains(rec.bodyString(), "data:") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-done

		body := rec.bodyString()
		if !strings.Contains(body, "data:") {
			t.Fatalf("expected immediate frame, body:\n%s", body)
		}
		wantID := "id: " + ts.UTC().Format(time.RFC3339Nano)
		if !strings.Contains(body, wantID) {
			t.Fatalf("want %q in:\n%s", wantID, body)
		}
	})

	t.Run("equalLastEventID", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Last-Event-ID", ts.UTC().Format(time.RFC3339Nano))
		rec := httptest.NewRecorder()

		done := make(chan struct{})
		go func() {
			defer close(done)
			h.streamMetricsSSE(rec, req, &row, metricsSSEOptions{emitInterval: time.Hour, pingInterval: time.Hour})
		}()

		time.Sleep(30 * time.Millisecond)
		cancel()
		<-done

		body := rec.Body.String()
		if strings.Contains(body, "data:") {
			t.Fatalf("equal Last-Event-ID must not replay snapshot, body:\n%s", body)
		}
	})
}

func TestHandleMetricsStream_WebSocketStillUpgrades(t *testing.T) {
	t.Parallel()

	const ns = "ns-ws-metrics"
	st := newFakeDeploymentStore()
	st.byNS[ns] = store.DeploymentRow{Namespace: ns, Template: "url-shortener"}
	collector := &metrics.MetricsCollector{}
	h := NewHandler(nil, st, nil, nil, collector, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/deployments/" + ns + "/metrics"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("websocket response must not be SSE")
	}

	_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}

func TestHandleLogsStream_SSEWritesLine(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	sink := &logsSSELineSink{w: rec}
	ts := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	line := logs.LogLine{
		Timestamp: ts,
		Component: "app-server",
		Level:     "info",
		Message:   "hello sse",
	}
	if err := sink.WriteLine(line); err != nil {
		t.Fatalf("WriteLine: %v", err)
	}
	if err := sink.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "data:") {
		t.Fatalf("missing data line:\n%s", body)
	}
	if !strings.Contains(body, "hello sse") {
		t.Fatalf("missing message in payload:\n%s", body)
	}
	if !strings.Contains(body, ": ping") {
		t.Fatalf("missing ping:\n%s", body)
	}
}

func TestStreamConnectionsGauge(t *testing.T) {
	t.Parallel()

	const ns = "ns-gauge"
	m := newControlPlaneMetrics()
	st := newFakeDeploymentStore()
	st.byNS[ns] = store.DeploymentRow{Namespace: ns, Template: "url-shortener"}
	h := &Handler{store: st, cpMetrics: m}

	labels := map[string]string{"transport": "sse", "kind": "metrics"}
	if gaugeValue(t, m, "cloud9_stream_connections", labels) != 0 {
		t.Fatal("want 0 before stream")
	}

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/deployments/"+ns+"/metrics", nil).WithContext(ctx)
	req.SetPathValue("id", ns)
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()

	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		h.handleMetricsStream(rec, req)
	}()

	waitUntil(t, 500*time.Millisecond, func() bool {
		return gaugeValue(t, m, "cloud9_stream_connections", labels) == 1
	})

	cancel()
	<-streamDone

	waitUntil(t, 500*time.Millisecond, func() bool {
		return gaugeValue(t, m, "cloud9_stream_connections", labels) == 0
	})
}

// sseTestResponse is an http.ResponseWriter safe to read while streamMetricsSSE writes.
// httptest.ResponseRecorder is not safe for that pattern under -race.
type sseTestResponse struct {
	mu     sync.Mutex
	header http.Header
	body   bytes.Buffer
	code   int
}

func newSSETestResponse() *sseTestResponse {
	return &sseTestResponse{header: make(http.Header)}
}

func (r *sseTestResponse) Header() http.Header { return r.header }

func (r *sseTestResponse) WriteHeader(code int) {
	r.mu.Lock()
	r.code = code
	r.mu.Unlock()
}

func (r *sseTestResponse) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.Write(p)
}

func (r *sseTestResponse) Flush() {}

func (r *sseTestResponse) bodyString() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.String()
}

func waitCollectPendingCleared(t *testing.T, c *metrics.MetricsCollector, ns string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		dm := c.Latest(ns)
		if dm != nil && !dm.CollectPending {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for publisher collect")
}

func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func gaugeValue(t *testing.T, m *controlPlaneMetrics, name string, labels map[string]string) float64 {
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
			return metric.GetGauge().GetValue()
		}
	}
	return 0
}
