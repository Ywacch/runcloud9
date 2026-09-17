package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/wskeepalive"
)

const (
	collectInterval = 2 * time.Second
	maxMessageSize  = 512 * 1024 // 512KB
)

type streamConfig struct {
	emitInterval time.Duration
}

// StreamMetrics subscribes to the session publisher and streams last-known-good
// snapshots to a WebSocket until the context is cancelled or the connection closes.
//
// topo summarizes the saved deployment topology (zero value ok); passed to EnsurePublisher
// so Collect uses the correct storytelling flags.
//
// gorilla/websocket connections are NOT goroutine-safe for concurrent writes.
// We use a single writer goroutine that owns the connection. The emitter goroutine
// sends to a buffered channel; the writer goroutine reads and sends. This is the
// standard Go WebSocket pattern — never call conn.WriteJSON from multiple goroutines.
func StreamMetrics(ctx context.Context, conn *websocket.Conn, collector *MetricsCollector, namespace, template string, topo TopologyFlags) {
	if collector != nil {
		collector.EnsurePublisher(namespace, template, topo)
	}
	streamMetrics(ctx, conn, namespace, func() *DeploymentMetrics {
		if collector == nil {
			return nil
		}
		return collector.Latest(namespace)
	}, streamConfig{emitInterval: collectInterval})
}

func streamMetrics(ctx context.Context, conn *websocket.Conn, namespace string, latest func() *DeploymentMetrics, cfg streamConfig) {
	if cfg.emitInterval <= 0 {
		cfg.emitInterval = collectInterval
	}

	// Hijacked request contexts often stay live after the client closes the
	// socket. Cancel when the reader or writer exits so the subscriber stops.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	writeCh := make(chan *DeploymentMetrics, 2)

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

	emit := func() {
		dm := latest()
		if dm == nil {
			return
		}
		select {
		case writeCh <- dm:
		case <-ctx.Done():
		default:
			// Writer is slow; skip rather than drop-oldest and burst later.
		}
	}

	emit()

	emitTicker := time.NewTicker(cfg.emitInterval)
	defer emitTicker.Stop()

	for {
		select {
		case <-emitTicker.C:
			emit()
		case <-ctx.Done():
			return
		}
	}
}
