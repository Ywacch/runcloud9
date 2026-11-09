package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

// scrapeNginxStubActiveConnections returns active connection count from lb:80/nginx_status, or 0 on any failure.
func (c *MetricsCollector) scrapeNginxStubActiveConnections(ctx context.Context, namespace string) float64 {
	if c == nil || c.scrapeClient == nil {
		return 0
	}
	release, ok := c.beginSideWorkScrape(ctx, namespace)
	if !ok {
		return 0
	}
	defer release()
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

// parseRabbitMQMetricsText parses rabbitmq_prometheus plugin exposition (used in tests).
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

type consistencySamplesPayload struct {
	Samples []struct {
		Key       string `json:"key"`
		Source    string `json:"source"`
		Served    string `json:"served"`
		Truth     string `json:"truth"`
		RequestID string `json:"requestId"`
	} `json:"samples"`
	ReplicaSamples []struct {
		Kind          string `json:"kind"`
		TableOrKey    string `json:"tableOrKey"`
		ReplicaHost   string `json:"replicaHost"`
		WriteToken    string `json:"writeToken"`
		ObservedToken string `json:"observedToken"`
		LagBytes      int64  `json:"lagBytes"`
		RequestID     string `json:"requestId"`
		ClientIDHash  string `json:"clientIdHash"`
	} `json:"replicaSamples"`
}

// scrapeConsistencySamples returns cache and replica violation rows from app-server.
func (c *MetricsCollector) scrapeConsistencySamples(ctx context.Context, namespace string) (cacheStaleSample, []replicaViolationSample) {
	ctx = withPromCaller(ctx, "scrape")
	if c == nil || c.scrapeClient == nil || namespace == "" {
		return cacheStaleSample{}, nil
	}
	release, ok := c.beginSideWorkScrape(ctx, namespace)
	if !ok {
		return cacheStaleSample{}, nil
	}
	defer release()
	u := fmt.Sprintf("http://app-server.%s.svc.cluster.local:8080/consistency/samples", namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return cacheStaleSample{}, nil
	}
	resp, err := c.scrapeClient.Do(req)
	if err != nil {
		return cacheStaleSample{}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return cacheStaleSample{}, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return cacheStaleSample{}, nil
	}
	cache, replicas := parseConsistencySamplesJSON(body)
	return cache, replicas
}

func parseConsistencySamplesJSON(body []byte) (cacheStaleSample, []replicaViolationSample) {
	var payload consistencySamplesPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return cacheStaleSample{}, nil
	}
	var cache cacheStaleSample
	if len(payload.Samples) > 0 {
		row := payload.Samples[0]
		if strings.TrimSpace(row.Key) != "" {
			cache = cacheStaleSample{
				Key:       row.Key,
				Served:    row.Served,
				Truth:     row.Truth,
				RequestID: row.RequestID,
			}
		}
	}
	if len(payload.ReplicaSamples) == 0 {
		return cache, nil
	}
	replicas := make([]replicaViolationSample, 0, len(payload.ReplicaSamples))
	for _, row := range payload.ReplicaSamples {
		if strings.TrimSpace(row.Kind) == "" {
			continue
		}
		replicas = append(replicas, replicaViolationSample{
			Kind:          row.Kind,
			TableOrKey:    row.TableOrKey,
			ReplicaHost:   row.ReplicaHost,
			WriteToken:    row.WriteToken,
			ObservedToken: row.ObservedToken,
			LagBytes:      row.LagBytes,
			RequestID:     row.RequestID,
			ClientIDHash:  row.ClientIDHash,
		})
	}
	return cache, replicas
}

// scrapeAppTrafficConsistency reads cumulative readrouter counters from Prometheus for student RYW/monotonic.
// CacheDbStale stays on consistency_read_stale_total (E4); this scrapes replica consistency only.
func (c *MetricsCollector) scrapeAppTrafficConsistency(ctx context.Context, nsSel string) appTrafficConsistency {
	ctx = withPromCaller(ctx, "scrape")
	out := appTrafficConsistency{}
	if c == nil || nsSel == "" {
		return out
	}
	ryw, _, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(ryw_violations_total{%s})`, nsSel))
	mono, _, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(monotonic_violations_total{%s})`, nsSel))
	redirects, _, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(ryw_leader_redirects_total{%s})`, nsSel))
	stale, _, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(replica_stale_reads_total{%s})`, nsSel))
	out.RYWTotal = ryw
	out.MonoTotal = mono
	out.LeaderRedirects = redirects
	out.ReplicaStaleTotal = stale

	byReplicaCum := c.queryMultiResult(ctx, fmt.Sprintf(`sum(db_read_total{%s}) by (replica)`, nsSel), "replica")
	out.LeaderReads = int64(byReplicaCum["leader"] + 0.5)
	var replicaCum float64
	for host, v := range byReplicaCum {
		if host != "leader" {
			replicaCum += v
		}
	}
	out.ReplicaReads = int64(replicaCum + 0.5)

	threadReads, readsPresent, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(social_thread_reads_total{%s})`, nsSel))
	threadOrphans, orphansPresent, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(social_thread_orphan_reads_total{%s})`, nsSel))
	out.ThreadReadsTotal = int64(threadReads + 0.5)
	out.ThreadOrphanReadsTotal = int64(threadOrphans + 0.5)
	out.ThreadCountersPresent = readsPresent || orphansPresent

	rates := c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (replica) (rate(db_read_total{%s}%s))`,
		nsSel, promRateRange(),
	), "replica")
	if len(rates) > 0 {
		out.ReadRateByReplica = rates
	}
	return out
}

type traceSamplesPayload struct {
	Samples []trace.JourneySample `json:"samples"`
}

// scrapeTraceSamples returns sampled request journeys from app-server.
func (c *MetricsCollector) scrapeTraceSamples(ctx context.Context, namespace string) []trace.JourneySample {
	if c == nil || c.scrapeClient == nil || namespace == "" {
		return nil
	}
	u := fmt.Sprintf("http://app-server.%s.svc.cluster.local:8080/trace/samples", namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	resp, err := c.scrapeClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
	if err != nil {
		return nil
	}
	return parseTraceSamplesJSON(body)
}

func parseTraceSamplesJSON(body []byte) []trace.JourneySample {
	var payload traceSamplesPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil
	}
	if len(payload.Samples) == 0 {
		return nil
	}
	out := make([]trace.JourneySample, 0, len(payload.Samples))
	for _, s := range payload.Samples {
		if strings.TrimSpace(s.RequestID) == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}
