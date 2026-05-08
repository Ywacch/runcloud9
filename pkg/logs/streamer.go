package logs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	logWriteWait  = 10 * time.Second
	logPongWait   = 60 * time.Second
	logPingPeriod = (logPongWait * 9) / 10
	logBufferSize = 256
	logTailLines  = int64(100) // send last 100 lines on connect, then follow
)

type componentLogTarget struct {
	labelSelector string
	containerName string
}

type podLogStream struct {
	component     string
	podName       string
	containerName string
}

// componentLogTargets maps each logical component to one or more pod/container
// targets. Cache intentionally supports multiple topologies:
// - cache-aside: app=redis
// - proxy modes: app=cache-proxy + app=redis-internal
var componentLogTargets = map[string][]componentLogTarget{
	"db":         {{labelSelector: "app=postgres", containerName: "postgres"}},
	"cache":      {{labelSelector: "app=redis", containerName: "redis"}, {labelSelector: "app=cache-proxy", containerName: "cache-proxy"}, {labelSelector: "app=redis-internal", containerName: "redis"}},
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
func StreamLogs(ctx context.Context, conn *websocket.Conn, clientset kubernetes.Interface, namespace string) {
	writeCh := make(chan LogLine, logBufferSize)

	// Writer goroutine — sole owner of conn.WriteJSON.
	go func() {
		ticker := time.NewTicker(logPingPeriod)
		defer ticker.Stop()
		defer conn.Close()

		for {
			select {
			case line := <-writeCh:
				_ = conn.SetWriteDeadline(time.Now().Add(logWriteWait))
				if err := conn.WriteJSON(line); err != nil {
					slog.Warn("log ws write failed", "namespace", namespace, "error", err)
					return
				}
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(logWriteWait))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			case <-ctx.Done():
				_ = conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
		}
	}()

	// Reader goroutine — handles pong, detects disconnect.
	go func() {
		conn.SetReadLimit(512 * 1024)
		_ = conn.SetReadDeadline(time.Now().Add(logPongWait))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(logPongWait))
			return nil
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Start one log-streaming goroutine per discovered pod stream target.
	for _, stream := range discoverPodLogStreams(ctx, clientset, namespace) {
		go streamPodLogs(ctx, clientset, namespace, stream.podName, stream.component, stream.containerName, writeCh)
	}

	<-ctx.Done()
}

func discoverPodLogStreams(ctx context.Context, clientset kubernetes.Interface, namespace string) []podLogStream {
	streams := make([]podLogStream, 0, len(componentLogTargets))
	seen := make(map[string]struct{})
	for component, targets := range componentLogTargets {
		for _, target := range targets {
			pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: target.labelSelector,
			})
			if err != nil || len(pods.Items) == 0 {
				continue
			}
			for _, pod := range pods.Items {
				key := fmt.Sprintf("%s|%s|%s", component, pod.Name, target.containerName)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				streams = append(streams, podLogStream{
					component:     component,
					podName:       pod.Name,
					containerName: target.containerName,
				})
			}
		}
	}
	return streams
}

// streamPodLogs streams logs from a single pod and parses each line into a LogLine.
// Follow=true keeps the stream open — new log lines are pushed as they
// are written to stdout. TailLines sends the last N lines on connect so the user
// sees recent history before live streaming begins.
// containerName specifies which container to stream from in multi-container pods.
func streamPodLogs(ctx context.Context, clientset kubernetes.Interface, namespace, podName, component, containerName string, writeCh chan<- LogLine) {
	tailLines := logTailLines
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: containerName,
		Follow:    true,
		TailLines: &tailLines,
	})

	stream, err := req.Stream(ctx)
	if err != nil {
		slog.Warn("failed to open log stream", "pod", podName, "error", err)
		return
	}
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()
		logLine := parseLine(line, component)

		// Non-blocking send.
		// UNDERSTAND: if the WebSocket writer is slow (client tab backgrounded),
		// we drop log lines rather than blocking pod log scanning.
		select {
		case writeCh <- logLine:
		default:
		}
	}

	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		slog.Warn("log scanner error", "pod", podName, "error", err)
	}
}

// parseLine attempts to parse a JSON log line (structured logging).
// Falls back to a plain-text LogLine if parsing fails.
// UNDERSTAND: app-server emits JSON (slog.NewJSONHandler). Postgres and Redis
// emit plain text. We handle both gracefully.
func parseLine(line, component string) LogLine {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(line), &parsed); err == nil {
		ll := LogLine{
			Component: component,
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
		Level:     "info",
		Message:   line,
	}
}
