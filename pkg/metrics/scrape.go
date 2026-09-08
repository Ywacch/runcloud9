package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type rabbitCounterSnapshot struct {
	received  float64
	delivered float64
	at        time.Time
}

// scrapeNginxStubActiveConnections returns active connection count from lb:80/nginx_status, or 0 on any failure.
func (c *MetricsCollector) scrapeNginxStubActiveConnections(ctx context.Context, namespace string) float64 {
	if c == nil || c.scrapeClient == nil {
		return 0
	}
	u := fmt.Sprintf("http://lb.%s.svc.cluster.local/nginx_status", namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0
	}
	resp, err := c.scrapeClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return 0
	}
	return parseNginxActiveConnections(string(body))
}

// scrapeRabbitMQPrometheus reads RabbitMQ native Prometheus metrics from :15692.
// Rates are computed as counter deltas between collection ticks.
func (c *MetricsCollector) scrapeRabbitMQPrometheus(ctx context.Context, namespace string) (depth, produce, consume, ready, unacked, memUsed, memLimit float64) {
	var receivedTotal, deliveredTotal float64
	if c == nil || c.scrapeClient == nil {
		return 0, 0, 0, 0, 0, 0, 0
	}
	u := fmt.Sprintf("http://mq.%s.svc.cluster.local:15692/metrics", namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0
	}
	resp, err := c.scrapeClient.Do(req)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, 0, 0, 0, 0, 0
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0
	}
	depth, ready, unacked, receivedTotal, deliveredTotal, memUsed, memLimit = parseRabbitMQMetricsText(string(body))
	produce, consume = c.rabbitCounterRates(namespace, receivedTotal, deliveredTotal, time.Now())
	return depth, produce, consume, ready, unacked, memUsed, memLimit
}

func parseRabbitMQMetricsText(raw string) (depth, ready, unacked, receivedTotal, deliveredTotal, memUsed, memLimit float64) {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		if idx := strings.IndexByte(name, '{'); idx >= 0 {
			name = name[:idx]
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		switch name {
		case "rabbitmq_queue_messages":
			depth += v
		case "rabbitmq_queue_messages_ready":
			ready += v
		case "rabbitmq_queue_messages_unacked":
			unacked += v
		case "rabbitmq_global_messages_received_total":
			receivedTotal += v
		case "rabbitmq_global_messages_delivered_consume_auto_ack_total":
			deliveredTotal += v
		case "rabbitmq_process_resident_memory_bytes":
			memUsed += v
		case "rabbitmq_resident_memory_limit_bytes":
			memLimit += v
		}
	}
	return depth, ready, unacked, receivedTotal, deliveredTotal, memUsed, memLimit
}

func (c *MetricsCollector) rabbitCounterRates(namespace string, receivedTotal, deliveredTotal float64, now time.Time) (produce, consume float64) {
	if c == nil {
		return 0, 0
	}
	c.mqRateMu.Lock()
	defer c.mqRateMu.Unlock()
	prev, ok := c.mqRates[namespace]
	c.mqRates[namespace] = rabbitCounterSnapshot{
		received:  receivedTotal,
		delivered: deliveredTotal,
		at:        now,
	}
	if !ok {
		return 0, 0
	}
	dt := now.Sub(prev.at).Seconds()
	if dt <= 0 {
		return 0, 0
	}
	produce = (receivedTotal - prev.received) / dt
	consume = (deliveredTotal - prev.delivered) / dt
	if produce < 0 {
		produce = 0
	}
	if consume < 0 {
		consume = 0
	}
	return produce, consume
}

type consistencySamplesPayload struct {
	Samples []struct {
		Key       string `json:"key"`
		Source    string `json:"source"`
		Served    string `json:"served"`
		Truth     string `json:"truth"`
		RequestID string `json:"requestId"`
	} `json:"samples"`
}

// scrapeConsistencySample returns the newest stale-read evidence row from app-server, or zero value on failure.
func (c *MetricsCollector) scrapeConsistencySample(ctx context.Context, namespace string) cacheStaleSample {
	if c == nil || c.scrapeClient == nil || namespace == "" {
		return cacheStaleSample{}
	}
	u := fmt.Sprintf("http://app-server.%s.svc.cluster.local:8080/consistency/samples", namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return cacheStaleSample{}
	}
	resp, err := c.scrapeClient.Do(req)
	if err != nil {
		return cacheStaleSample{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return cacheStaleSample{}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return cacheStaleSample{}
	}
	return parseConsistencySamplesJSON(body)
}

func parseConsistencySamplesJSON(body []byte) cacheStaleSample {
	var payload consistencySamplesPayload
	if err := json.Unmarshal(body, &payload); err != nil || len(payload.Samples) == 0 {
		return cacheStaleSample{}
	}
	row := payload.Samples[0]
	if strings.TrimSpace(row.Key) == "" {
		return cacheStaleSample{}
	}
	return cacheStaleSample{
		Key:       row.Key,
		Served:    row.Served,
		Truth:     row.Truth,
		RequestID: row.RequestID,
	}
}

// scrapeAppTrafficConsistency reads cumulative readrouter counters from Prometheus for student RYW/monotonic.
// CacheDbStale stays on consistency_read_stale_total (E4); this scrapes replica consistency only.
func (c *MetricsCollector) scrapeAppTrafficConsistency(ctx context.Context, nsSel string) appTrafficConsistency {
	out := appTrafficConsistency{}
	if c == nil || nsSel == "" {
		return out
	}
	ryw, _, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(ryw_violations_total{%s})`, nsSel))
	mono, _, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(monotonic_violations_total{%s})`, nsSel))
	redirects, _, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(ryw_leader_redirects_total{%s})`, nsSel))
	out.RYWTotal = ryw
	out.MonoTotal = mono
	out.LeaderRedirects = redirects

	byRoute := c.queryMultiResult(ctx, fmt.Sprintf(`sum(db_read_total{%s}) by (route)`, nsSel), "route")
	out.LeaderReads = int64(byRoute["leader"] + 0.5)
	out.ReplicaReads = int64(byRoute["replica"] + 0.5)
	return out
}
