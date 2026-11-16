package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/logs"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
)

var errSSEFlushUnsupported = errors.New("sse flush unsupported")

const (
	sseMetricsEmitInterval = 2 * time.Second
	sseMetricsPingInterval = 15 * time.Second
	sseLogsPingInterval    = 15 * time.Second
)

type metricsSSEOptions struct {
	emitInterval time.Duration
	pingInterval time.Duration
}

func wantsSSE(r *http.Request) bool {
	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		return false
	}
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	return true
}

func writeSSEComment(w http.ResponseWriter, text string) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return errSSEFlushUnsupported
	}
	if _, err := fmt.Fprintf(w, ": %s\n\n", text); err != nil {
		return fmt.Errorf("sse comment: %w", err)
	}
	flusher.Flush()
	return nil
}

func writeSSEEvent(w http.ResponseWriter, id string, data []byte) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return errSSEFlushUnsupported
	}
	if id != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", id); err != nil {
			return fmt.Errorf("sse id: %w", err)
		}
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return fmt.Errorf("sse data: %w", err)
	}
	flusher.Flush()
	return nil
}

func writeSSEHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
}

func parseLastEventID(r *http.Request) (time.Time, bool) {
	raw := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if raw == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func (h *Handler) streamMetricsSSE(w http.ResponseWriter, r *http.Request, row *store.DeploymentRow, opts metricsSSEOptions) {
	if _, ok := w.(http.Flusher); !ok {
		writeError(w, http.StatusInternalServerError, "sse flush unsupported")
		return
	}
	writeSSEHeaders(w)

	if opts.emitInterval <= 0 {
		opts.emitInterval = sseMetricsEmitInterval
	}
	if opts.pingInterval <= 0 {
		opts.pingInterval = sseMetricsPingInterval
	}

	namespace := row.Namespace
	template := row.Template
	design, _ := deployer.DesignFromMap(row.Design)
	topo := MetricsTopologyFromDeploymentDesign(design, template)
	if h.metricsCollector != nil {
		h.metricsCollector.EnsurePublisher(namespace, template, topo)
	}

	ctx := r.Context()
	lastID, hasLast := parseLastEventID(r)
	if hasLast {
		if dm := h.latestMetrics(namespace); dm != nil && dm.Timestamp.After(lastID) {
			h.writeMetricsSSEFrame(w, dm)
		}
	}

	emitTicker := time.NewTicker(opts.emitInterval)
	defer emitTicker.Stop()
	pingTicker := time.NewTicker(opts.pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pingTicker.C:
			if err := writeSSEComment(w, "ping"); err != nil {
				return
			}
		case <-emitTicker.C:
			dm := h.latestMetrics(namespace)
			if dm == nil {
				continue
			}
			h.writeMetricsSSEFrame(w, dm)
		}
	}
}

func (h *Handler) latestMetrics(namespace string) *metrics.DeploymentMetrics {
	if h.metricsCollector == nil {
		return nil
	}
	return h.metricsCollector.Latest(namespace)
}

func (h *Handler) writeMetricsSSEFrame(w http.ResponseWriter, dm *metrics.DeploymentMetrics) {
	payload, err := json.Marshal(dm)
	if err != nil {
		return
	}
	if h.metricsCollector != nil {
		h.metricsCollector.ObserveSerializedFrameBytes(len(payload))
	}
	id := dm.Timestamp.UTC().Format(time.RFC3339Nano)
	_ = writeSSEEvent(w, id, payload)
}

func (h *Handler) streamLogsSSE(w http.ResponseWriter, r *http.Request, namespace string) {
	if _, ok := w.(http.Flusher); !ok {
		writeError(w, http.StatusInternalServerError, "sse flush unsupported")
		return
	}
	writeSSEHeaders(w)
	sink := &logsSSELineSink{w: w}
	logs.StreamWithSink(r.Context(), sink, h.clientset, namespace, sseLogsPingInterval)
}

type logsSSELineSink struct {
	w  http.ResponseWriter
	mu sync.Mutex
}

func (s *logsSSELineSink) WriteLine(line logs.LogLine) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, err := json.Marshal(line)
	if err != nil {
		return fmt.Errorf("marshal log line: %w", err)
	}
	id := line.Timestamp.UTC().Format(time.RFC3339Nano)
	return writeSSEEvent(s.w, id, payload)
}

func (s *logsSSELineSink) Ping() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeSSEComment(s.w, "ping")
}

// streamMetricsWebSocket upgrades and streams metrics over WebSocket.
func (h *Handler) streamMetricsWebSocket(w http.ResponseWriter, r *http.Request, row *store.DeploymentRow) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}
	ctx, cancel := cancelWhenSocketCloses(r.Context(), conn)
	defer cancel()
	design, _ := deployer.DesignFromMap(row.Design)
	topo := MetricsTopologyFromDeploymentDesign(design, row.Template)
	metrics.StreamMetrics(ctx, conn, h.metricsCollector, row.Namespace, row.Template, topo)
	h.refreshWebsocketLiveGauge()
}

// streamLogsWebSocket upgrades and streams logs over WebSocket.
func (h *Handler) streamLogsWebSocket(w http.ResponseWriter, r *http.Request, namespace string) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("log ws upgrade failed: %v", err)
		return
	}
	ctx, cancel := cancelWhenSocketCloses(r.Context(), conn)
	defer cancel()
	logs.StreamLogs(ctx, conn, h.clientset, namespace)
}
