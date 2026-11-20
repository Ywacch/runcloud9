package logs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/placement"
	"github.com/ywacch/cloud9/pkg/wskeepalive"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	logBufferSize          = 256
	logTailLines           = int64(100)
	logRediscoverInterval  = 5 * time.Second
	logStreamRetryInterval = 2 * time.Second
)

type componentLogTarget struct {
	labelSelector string
	containerName string
}

type podLogStream struct {
	component     string
	nodeID        string
	podName       string
	containerName string
}

func (s podLogStream) key() string {
	return fmt.Sprintf("%s|%s|%s|%s", s.component, s.nodeID, s.podName, s.containerName)
}

// streamSet records which pod/container streams have already been started.
type streamSet struct {
	mu      sync.Mutex
	started map[string]struct{}
}

func newStreamSet() *streamSet {
	return &streamSet{started: make(map[string]struct{})}
}

// tryAdd reports whether this is the first time the stream has been seen.
func (s *streamSet) tryAdd(stream podLogStream) bool {
	key := stream.key()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.started[key]; exists {
		return false
	}
	s.started[key] = struct{}{}
	return true
}

func connectTailLines(needTail bool) *int64 {
	if !needTail {
		return nil
	}
	n := logTailLines
	return &n
}

// componentLogTargets maps each logical component to one or more pod/container
// targets. Cache intentionally supports multiple topologies:
// - cache-aside Redis: app=redis
// - cache-aside Memcached: app=memcached
// - proxy modes: app=cache-proxy + app=redis-internal
var componentLogTargets = map[string][]componentLogTarget{
	"db": {
		{labelSelector: "app=postgres", containerName: "postgres"},
		{labelSelector: "app=postgres-replica", containerName: "postgres-replica"},
	},
	"cache": {
		{labelSelector: "app=redis", containerName: "redis"},
		{labelSelector: "app=memcached", containerName: "memcached"},
		{labelSelector: "app=cache-proxy", containerName: "cache-proxy"},
		{labelSelector: "app=redis-internal", containerName: "redis"},
	},
	"app-server": {{labelSelector: "app=app-server", containerName: "app-server"}},
	"mq":         {{labelSelector: "app=rabbitmq", containerName: "rabbitmq"}},
	"worker":     {{labelSelector: "app=worker", containerName: "worker"}},
	"lb":         {{labelSelector: "app=lb", containerName: "nginx"}},
}

// StreamLogs finds all pods in the namespace, starts one goroutine per pod
// streaming logs, and writes parsed LogLines to the WebSocket connection.
//
// The K8s Logs API streams from a single pod at a time
// as there is no "stream all pods in a namespace" call. One goroutine per pod
// is the natural fit. All goroutines send to a shared channel; a single
// writer goroutine owns the WebSocket connection.
//
// Discovery is repeated while ctx is active so pods that appear after connect
// (Postgres replicas finishing init, late Redis) still get a follower.
func StreamLogs(ctx context.Context, conn *websocket.Conn, clientset kubernetes.Interface, namespace string, topo *placement.Topology) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sink := newWSLineSink(conn)
	go runLogWebSocketReader(ctx, conn, cancel)
	defer conn.Close()

	_ = streamLogsCore(ctx, sink, clientset, namespace, wskeepalive.PingPeriod, topo)
}

// StreamWithSink streams pod logs to sink until ctx is cancelled. The caller
// owns response headers for SSE.
func StreamWithSink(ctx context.Context, sink LineSink, clientset kubernetes.Interface, namespace string, pingInterval time.Duration, topo *placement.Topology) {
	if pingInterval <= 0 {
		pingInterval = wskeepalive.PingPeriod
	}
	_ = streamLogsCore(ctx, sink, clientset, namespace, pingInterval, topo)
}

func runLogWebSocketReader(ctx context.Context, conn *websocket.Conn, cancel context.CancelFunc) {
	defer cancel()
	conn.SetReadLimit(512 * 1024)
	_ = conn.SetReadDeadline(time.Now().Add(wskeepalive.PongWait))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(wskeepalive.PongWait))
		return nil
	})
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func streamLogsCore(ctx context.Context, sink LineSink, clientset kubernetes.Interface, namespace string, pingInterval time.Duration, topo *placement.Topology) error {
	writeCh := make(chan LogLine, logBufferSize)

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case line, ok := <-writeCh:
				if !ok {
					return
				}
				if err := sink.WriteLine(line); err != nil {
					slog.Warn("log stream write failed", "namespace", namespace, "error", err)
					return
				}
			case <-ticker.C:
				if err := sink.Ping(); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	defer func() {
		close(writeCh)
		<-writerDone
	}()

	started := newStreamSet()
	startNew := func(streams []podLogStream) {
		for _, stream := range streams {
			if !started.tryAdd(stream) {
				continue
			}
			s := stream
			go streamPodLogs(ctx, clientset, namespace, s.podName, s.component, s.nodeID, s.containerName, writeCh)
		}
	}

	startNew(discoverPodLogStreams(ctx, clientset, namespace, topo))

	ticker := time.NewTicker(logRediscoverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			startNew(discoverPodLogStreams(ctx, clientset, namespace, topo))
		}
	}
}

// streamPodLogs streams logs from a single pod and parses each line into a LogLine.
// Follow=true keeps the stream open — new log lines are pushed as they
// are written to stdout. TailLines sends the last N lines on connect so the user
// sees recent history before live streaming begins.
// containerName specifies which container to stream from in multi-container pods.
//
// GetLogs fails while an init container is still running. Retry until ctx is done
// so db/cache streams start after replicas finish basebackup.
func streamPodLogs(ctx context.Context, clientset kubernetes.Interface, namespace, podName, component, nodeID, containerName string, writeCh chan<- LogLine) {
	needTail := true
	for {
		if ctx.Err() != nil {
			return
		}
		opened, err := followPodLogs(ctx, clientset, namespace, podName, component, nodeID, containerName, connectTailLines(needTail), writeCh)
		if opened {
			needTail = false
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Warn("failed to open log stream", "pod", podName, "container", containerName, "error", err)
		}
		timer := time.NewTimer(logStreamRetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func followPodLogs(ctx context.Context, clientset kubernetes.Interface, namespace, podName, component, nodeID, containerName string, tailLines *int64, writeCh chan<- LogLine) (opened bool, err error) {
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: containerName,
		Follow:    true,
		TailLines: tailLines,
	})

	stream, err := req.Stream(ctx)
	if err != nil {
		return false, fmt.Errorf("open log stream: %w", err)
	}
	defer func() {
		_ = stream.Close()
	}()

	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		default:
		}

		line := scanner.Text()
		logLine := parseLine(line, component, nodeID, podName)

		select {
		case writeCh <- logLine:
		default:
		}
	}

	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return true, fmt.Errorf("log scanner: %w", err)
	}
	return true, nil
}

// parseLine attempts to parse a JSON log line (structured logging).
// Falls back to a plain-text LogLine if parsing fails.
// UNDERSTAND: app-server emits JSON (slog.NewJSONHandler). Postgres and Redis
// emit plain text. We handle both gracefully.
func parseLine(line, component, nodeID, podName string) LogLine {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(line), &parsed); err == nil {
		ll := LogLine{
			Component: component,
			NodeID:    nodeID,
			Pod:       podName,
			Timestamp: time.Now(),
			Level:     "info",
			Fields:    make(map[string]any),
		}

		if msg, ok := parsed["msg"].(string); ok {
			ll.Message = msg
		} else {
			ll.Message = line
		}
		if level, ok := parsed["level"].(string); ok {
			ll.Level = strings.ToLower(level)
		}
		if t, ok := parsed["time"].(string); ok {
			if parsedTime, err := time.Parse(time.RFC3339, t); err == nil {
				ll.Timestamp = parsedTime
			}
		}

		for k, v := range parsed {
			if k != "msg" && k != "level" && k != "time" {
				ll.Fields[k] = v
			}
		}
		return ll
	}

	return LogLine{
		Timestamp: time.Now(),
		Component: component,
		NodeID:    nodeID,
		Pod:       podName,
		Level:     "info",
		Message:   line,
	}
}
