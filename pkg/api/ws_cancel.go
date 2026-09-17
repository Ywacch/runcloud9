package api

import (
	"context"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/wskeepalive"
)

// cancelWhenSocketCloses returns a child context cancelled when the peer sends a
// WebSocket close frame. Hijacked HTTP request contexts often stay live after
// the browser drops the socket, which left log follows and metrics Collect
// hammering the Kubernetes API.
func cancelWhenSocketCloses(parent context.Context, conn *websocket.Conn) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	conn.SetCloseHandler(func(code int, text string) error {
		cancel()
		msg := websocket.FormatCloseMessage(code, text)
		deadline := time.Now().Add(wskeepalive.WriteWait)
		_ = conn.WriteControl(websocket.CloseMessage, msg, deadline)
		return nil
	})
	return ctx, cancel
}
