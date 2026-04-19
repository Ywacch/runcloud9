package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

const (
	collectInterval = 2 * time.Second
	writeWait       = 10 * time.Second
	pongWait        = 60 * time.Second
	pingPeriod      = (pongWait * 9) / 10
	maxMessageSize  = 512 * 1024 // 512KB
)

// StreamMetrics collects metrics every 2s and streams them to a WebSocket connection
// until the context is cancelled or the connection closes.
//
// gorilla/websocket connections are NOT goroutine-safe for concurrent writes.
// We use a single writer goroutine that owns the connection. The collector goroutine
// sends to a buffered channel; the writer goroutine reads and sends. This is the
// standard Go WebSocket pattern — never call conn.WriteJSON from multiple goroutines.
func StreamMetrics(ctx context.Context, conn *websocket.Conn, collector *MetricsCollector, namespace, template string) {
	writeCh := make(chan *DeploymentMetrics, 4)

	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		defer conn.Close()

		for {
			select {
			case <-ctx.Done():
				_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
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
				_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := conn.WriteJSON(metrics); err != nil {
					slog.Warn("metrics ws write failed", "namespace", namespace, "error", err)
					return
				}
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()

	go func() {
		conn.SetReadLimit(maxMessageSize)
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(collectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			col := collector
			if col == nil {
				col = NewMetricsCollector("", nil, nil)
			}
			dm, err := col.Collect(ctx, namespace, template)
			if err != nil {
				slog.Warn("metrics collect failed", "namespace", namespace, "error", err)
				continue
			}
			// non-blocking send with drop-oldest backpressure.
			select {
			case writeCh <- dm:
			default:
				select {
				case <-writeCh:
				default:
				}
				select {
				case writeCh <- dm:
				default:
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
