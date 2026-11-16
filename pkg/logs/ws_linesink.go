package logs

import (
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/wskeepalive"
)

type wsLineSink struct {
	conn *websocket.Conn
}

func newWSLineSink(conn *websocket.Conn) *wsLineSink {
	return &wsLineSink{conn: conn}
}

func (s *wsLineSink) WriteLine(line LogLine) error {
	_ = s.conn.SetWriteDeadline(time.Now().Add(wskeepalive.WriteWait))
	return s.conn.WriteJSON(line)
}

func (s *wsLineSink) Ping() error {
	_ = s.conn.SetWriteDeadline(time.Now().Add(wskeepalive.WriteWait))
	if err := s.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		return fmt.Errorf("logs ws ping: %w", err)
	}
	return nil
}
