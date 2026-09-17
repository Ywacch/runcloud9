package metrics

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/wskeepalive"
)

const (
	collectInterval = 2 * time.Second
	maxMessageSize  = 512 * 1024 // 512KB
)

// snapshotCollector fetches one DeploymentMetrics snapshot for WS streaming.
type snapshotCollector func(ctx context.Context) (*DeploymentMetrics, error)

type streamConfig struct {
	emitInterval time.Duration
	collectPause time.Duration
}

// StreamMetrics collects metrics every 2s and streams them to a WebSocket connection
// until the context is cancelled or the connection closes.
//
// topo summarizes the saved deployment topology (zero value ok); passed to Collect for storytelling.
//
// Collection runs asynchronously from emission: a background goroutine refreshes the
// latest snapshot while a ticker emits at a steady cadence. Slow PromQL batches no
// longer stall the emit loop or trigger drop-oldest bursts on writeCh.
//
// gorilla/websocket connections are NOT goroutine-safe for concurrent writes.
// We use a single writer goroutine that owns the connection. The emitter goroutine
// sends to a buffered channel; the writer goroutine reads and sends. This is the
// standard Go WebSocket pattern — never call conn.WriteJSON from multiple goroutines.
func StreamMetrics(ctx context.Context, conn *websocket.Conn, collector *MetricsCollector, namespace, template string, topo TopologyFlags) {
	col := collector
	if col == nil {
		col = NewMetricsCollector("", nil, nil, nil, nil)
	}
	collect := func(ctx context.Context) (*DeploymentMetrics, error) {
		return col.Collect(ctx, namespace, template, topo)
	}
	streamMetrics(ctx, conn, namespace, collect, streamConfig{
		emitInterval: collectInterval,
		collectPause: collectInterval,
	})
}

func streamMetrics(ctx context.Context, conn *websocket.Conn, namespace string, collect snapshotCollector, cfg streamConfig) {
	if cfg.emitInterval <= 0 {
		cfg.emitInterval = collectInterval
	}
	if cfg.collectPause <= 0 {
		cfg.collectPause = collectInterval
	}

	// Hijacked request contexts often stay live after the client closes the
	// socket. Cancel when the reader or writer exits so Collect stops hitting
	// the Kubernetes API.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	writeCh := make(chan *DeploymentMetrics, 2)
	var latestSnapshot atomic.Pointer[DeploymentMetrics]

	go func() {
		defer cancel()
		ticker := time.NewTicker(wskeepalive.PingPeriod)
		defer ticker.Stop()
		defer conn.Close()

		for {
			select {
			case <-ctx.Done():
				_ = conn.SetWriteDeadline(time.Now().Add(wskeepalive.WriteWait))
				_ = conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			case metrics, ok := <-writeCh:
				if !ok {
					return
				}
				if metrics == nil {
					continue
				}
				_ = conn.SetWriteDeadline(time.Now().Add(wskeepalive.WriteWait))
				if err := conn.WriteJSON(metrics); err != nil {
					slog.Warn("metrics ws write failed", "namespace", namespace, "error", err)
					return
				}
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(wskeepalive.WriteWait))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()

	go func() {
		defer cancel()
		conn.SetReadLimit(maxMessageSize)
		_ = conn.SetReadDeadline(time.Now().Add(wskeepalive.PongWait))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(wskeepalive.PongWait))
			return nil
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			start := time.Now()
			dm, err := collect(ctx)
			if err != nil {
				slog.Warn("metrics collect failed", "namespace", namespace, "error", err)
			} else if dm != nil {
				latestSnapshot.Store(dm)
			}
			slog.Debug("metrics collect finished",
				"namespace", namespace,
				"duration_ms", time.Since(start).Milliseconds(),
				"ok", err == nil && dm != nil,
			)

			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.collectPause):
			}
		}
	}()

	emitTicker := time.NewTicker(cfg.emitInterval)
	defer emitTicker.Stop()

	for {
		select {
		case <-emitTicker.C:
			dm := latestSnapshot.Load()
			if dm == nil {
				continue
			}
			select {
			case writeCh <- dm:
			case <-ctx.Done():
				return
			default:
				// Writer is slow; skip this tick rather than drop-oldest and burst later.
			}
		case <-ctx.Done():
			return
		}
	}
}
