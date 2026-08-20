package logs

import "time"

// LogLine is one log entry streamed to the frontend.
// UNDERSTAND: we parse structured JSON logs from the pod stdout.
// For pods not emitting JSON (e.g. postgres, redis), we wrap the raw line
// in a LogLine with component set and level defaulting to "info".
type LogLine struct {
	Timestamp time.Time      `json:"timestamp"`
	Component string         `json:"component"` // "db" | "cache" | "app-server"
	Pod       string         `json:"pod,omitempty"`
	Level     string         `json:"level"` // "info" | "warn" | "error" | "debug"
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"` // extra structured fields
}
