// Package wskeepalive defines WebSocket ping/pong timing for API streaming handlers.
//
// GCE HTTP(S) load balancers use a 30s default backend service timeout. Keepalive
// pings must fire below that window so connections stay warm through the LB path.
// BackendConfig on cloud9-api raises the cap; these timings are defense in depth.
package wskeepalive

import "time"

const (
	// PongWait is how long the server waits for client pong before closing the read side.
	PongWait = 30 * time.Second
	// PingPeriod is how often the server sends WebSocket ping control frames.
	PingPeriod = 25 * time.Second
	// WriteWait is the deadline for each WS write (data or control).
	WriteWait = 10 * time.Second
)
