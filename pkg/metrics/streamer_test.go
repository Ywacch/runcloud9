package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestStreamMetrics_ObservesFrameBytes(t *testing.T) {
	t.Parallel()

	dm := &DeploymentMetrics{
		Timestamp:  time.Now(),
		Namespace:  "test-ns",
		Components: map[string]ComponentMetrics{},
	}
	payload, err := json.Marshal(dm)
	if err != nil {
		t.Fatal(err)
	}
	wantLen := len(payload)

	var got int
	mc := &MetricsCollector{
		frameBytesObserver: func(n int) {
			got = n
		},
	}

	latest := func() *DeploymentMetrics { return dm }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	srv, url := newMetricsWSServer(t, func(conn *websocket.Conn) {
		streamMetrics(ctx, conn, "test-ns", latest, streamConfig{
			emitInterval: time.Second,
		}, mc)
	})
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != wantLen {
		t.Fatalf("frame bytes: want %d, got %d", wantLen, got)
	}
}

func TestStreamMetrics_AsyncCollect_SteadyCadence(t *testing.T) {
	t.Parallel()

	var seq atomic.Int64
	latest := func() *DeploymentMetrics {
		n := seq.Add(1)
		return &DeploymentMetrics{
			Timestamp: time.Unix(n, 0),
			Namespace: "test-ns",
		}
	}

	intervals := runStreamMetricsWS(t, latest, streamConfig{
		emitInterval: 200 * time.Millisecond,
	}, 10, 0)

	const want = 200 * time.Millisecond
	const tolerance = 500 * time.Millisecond
	for i, d := range intervals {
		if d < want-tolerance || d > want+tolerance {
			t.Fatalf("frame %d interval %v outside [%v, %v]", i, d, want-tolerance, want+tolerance)
		}
	}
}

func TestStreamMetrics_SlowCollect_NoBackpressureDrop(t *testing.T) {
	t.Parallel()

	var seq atomic.Int64
	latest := func() *DeploymentMetrics {
		n := seq.Add(1)
		return &DeploymentMetrics{
			Timestamp: time.Unix(n, 0),
			Namespace: "test-ns",
		}
	}

	received := runStreamMetricsWSCount(t, latest, streamConfig{
		emitInterval: 100 * time.Millisecond,
	}, 2*time.Second)

	if received < 5 {
		t.Fatalf("expected steady frames while latest is always available, got %d", received)
	}
}

func TestStreamMetrics_ClientClose_DoesNotRequireCollect(t *testing.T) {
	t.Parallel()

	parent := context.Background()
	streamDone := make(chan struct{})
	latest := func() *DeploymentMetrics {
		return &DeploymentMetrics{
			Timestamp: time.Now(),
			Namespace: "test-ns",
		}
	}

	srv, url := newMetricsWSServer(t, func(conn *websocket.Conn) {
		defer close(streamDone)
		streamMetrics(parent, conn, "test-ns", latest, streamConfig{
			emitInterval: 50 * time.Millisecond,
		}, nil)
	})
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("expected initial frame: %v", err)
	}

	_ = conn.Close()

	select {
	case <-streamDone:
	case <-time.After(3 * time.Second):
		t.Fatal("streamMetrics did not exit after client closed the socket")
	}
}

func TestStreamMetrics_EmitsPendingBeforeCollect(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	latest := func() *DeploymentMetrics {
		if calls.Add(1) == 1 {
			return &DeploymentMetrics{
				Timestamp:      time.Now(),
				Namespace:      "test-ns",
				Components:     map[string]ComponentMetrics{},
				CollectPending: true,
			}
		}
		return &DeploymentMetrics{
			Timestamp:  time.Now(),
			Namespace:  "test-ns",
			Components: map[string]ComponentMetrics{},
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	srv, url := newMetricsWSServer(t, func(conn *websocket.Conn) {
		streamMetrics(ctx, conn, "test-ns", latest, streamConfig{
			emitInterval: 100 * time.Millisecond,
		}, nil)
	})
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var dm DeploymentMetrics
	if err := json.Unmarshal(data, &dm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !dm.CollectPending {
		t.Fatalf("expected first frame collectPending true, got %+v", dm)
	}
}

func TestStreamMetrics_ContextCancel_CleanShutdown(t *testing.T) {
	t.Parallel()

	latest := func() *DeploymentMetrics {
		return &DeploymentMetrics{
			Timestamp: time.Now(),
			Namespace: "test-ns",
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		srv, url := newMetricsWSServer(t, func(conn *websocket.Conn) {
			streamMetrics(ctx, conn, "test-ns", latest, streamConfig{
				emitInterval: 100 * time.Millisecond,
			}, nil)
		})
		defer srv.Close()

		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			t.Errorf("dial: %v", err)
			return
		}
		defer conn.Close()

		time.Sleep(350 * time.Millisecond)
		cancel()

		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("streamMetrics did not exit after context cancel")
	}
}

func runStreamMetricsWS(t *testing.T, latest func() *DeploymentMetrics, cfg streamConfig, frames int, warmup time.Duration) []time.Duration {
	t.Helper()

	var intervals []time.Duration
	var last time.Time

	timeout := time.Duration(frames)*cfg.emitInterval + warmup + 2*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	srv, url := newMetricsWSServer(t, func(conn *websocket.Conn) {
		streamMetrics(ctx, conn, "test-ns", latest, cfg, nil)
	})
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	for len(intervals) < frames-1 {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var dm DeploymentMetrics
		if err := json.Unmarshal(data, &dm); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		now := time.Now()
		if !last.IsZero() {
			intervals = append(intervals, now.Sub(last))
		}
		last = now
	}

	return intervals
}

func runStreamMetricsWSCount(t *testing.T, latest func() *DeploymentMetrics, cfg streamConfig, duration time.Duration) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	srv, url := newMetricsWSServer(t, func(conn *websocket.Conn) {
		streamMetrics(ctx, conn, "test-ns", latest, cfg, nil)
	})
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	count := 0
	deadline := time.Now().Add(duration)
	for {
		if ctx.Err() != nil {
			return count
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return count
		}
		_ = conn.SetReadDeadline(time.Now().Add(remaining))
		_, data, err := conn.ReadMessage()
		if err != nil {
			return count
		}
		var dm DeploymentMetrics
		if err := json.Unmarshal(data, &dm); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		count++
	}
}

func newMetricsWSServer(t *testing.T, handler func(*websocket.Conn)) (*httptest.Server, string) {
	t.Helper()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		handler(conn)
	}))
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http")
}
