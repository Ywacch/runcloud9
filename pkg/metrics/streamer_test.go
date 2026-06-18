package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestStreamMetrics_AsyncCollect_SteadyCadence(t *testing.T) {
	t.Parallel()

	delays := []time.Duration{0, time.Second, 3 * time.Second}
	for _, delay := range delays {
		delay := delay
		name := delay.String()
		if delay == 0 {
			name = "instant"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var seq atomic.Int64
			collect := func(ctx context.Context) (*DeploymentMetrics, error) {
				if delay > 0 {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(delay):
					}
				}
				n := seq.Add(1)
				return &DeploymentMetrics{
					Timestamp: time.Unix(n, 0),
					Namespace: "test-ns",
				}, nil
			}

			intervals := runStreamMetricsWS(t, collect, streamConfig{
				emitInterval: 200 * time.Millisecond,
				collectPause: 200 * time.Millisecond,
			}, 10, delay)

			const want = 200 * time.Millisecond
			const tolerance = 500 * time.Millisecond
			for i, d := range intervals {
				if d < want-tolerance || d > want+tolerance {
					t.Fatalf("frame %d interval %v outside [%v, %v]", i, d, want-tolerance, want+tolerance)
				}
			}
		})
	}
}

func TestStreamMetrics_SlowCollect_NoBackpressureDrop(t *testing.T) {
	t.Parallel()

	var seq atomic.Int64
	collect := func(ctx context.Context) (*DeploymentMetrics, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(400 * time.Millisecond):
		}
		n := seq.Add(1)
		return &DeploymentMetrics{
			Timestamp: time.Unix(n, 0),
			Namespace: "test-ns",
		}, nil
	}

	received := runStreamMetricsWSCount(t, collect, streamConfig{
		emitInterval: 100 * time.Millisecond,
		collectPause: 100 * time.Millisecond,
	}, 2*time.Second)

	if received < 5 {
		t.Fatalf("expected steady frames during slow collect, got %d", received)
	}
}

func TestStreamMetrics_ContextCancel_CleanShutdown(t *testing.T) {
	t.Parallel()

	collect := func(ctx context.Context) (*DeploymentMetrics, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		return &DeploymentMetrics{
			Timestamp: time.Now(),
			Namespace: "test-ns",
		}, nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		srv, url := newMetricsWSServer(t, func(conn *websocket.Conn) {
			streamMetrics(ctx, conn, "test-ns", collect, streamConfig{
				emitInterval: 100 * time.Millisecond,
				collectPause: 100 * time.Millisecond,
			})
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

func runStreamMetricsWS(t *testing.T, collect snapshotCollector, cfg streamConfig, frames int, collectWarmup time.Duration) []time.Duration {
	t.Helper()

	var intervals []time.Duration
	var last time.Time

	timeout := time.Duration(frames)*cfg.emitInterval + collectWarmup + 2*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	srv, url := newMetricsWSServer(t, func(conn *websocket.Conn) {
		streamMetrics(ctx, conn, "test-ns", collect, cfg)
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

func runStreamMetricsWSCount(t *testing.T, collect snapshotCollector, cfg streamConfig, duration time.Duration) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	srv, url := newMetricsWSServer(t, func(conn *websocket.Conn) {
		streamMetrics(ctx, conn, "test-ns", collect, cfg)
	})
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	count := 0
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if isTransientWSReadErr(err) {
				continue
			}
			break
		}
		var dm DeploymentMetrics
		if err := json.Unmarshal(data, &dm); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		count++
	}
	return count
}

// isTransientWSReadErr reports whether a WS read error is a read deadline timeout
// where the connection may still be usable. Do not match "deadline" as a substring:
// "context deadline exceeded" marks the conn failed and the next read panics.
func isTransientWSReadErr(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
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
