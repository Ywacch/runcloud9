package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func dbAppSQLRateByTargetQuery(appSel, rangeVec string) string {
	return fmt.Sprintf(
		`sum by (target) (rate(db_query_duration_seconds_count{%s}%s))`,
		appSel, rangeVec,
	)
}

func dbAppSQLTotalByTargetQuery(appSel string) string {
	return fmt.Sprintf(
		`sum by (target) (db_query_duration_seconds_count{%s})`,
		appSel,
	)
}

func dbAppSQLTargetLatencyQuantile(ctx context.Context, c *MetricsCollector, appSel, rangeVec string, quantile float64) map[string]float64 {
	if c == nil {
		return nil
	}
	q := fmt.Sprintf(
		`histogram_quantile(%.2f, sum by (target, le) (rate(db_query_duration_seconds_bucket{%s}%s)))`,
		quantile, appSel, rangeVec,
	)
	return c.queryMultiResult(ctx, q, "target")
}

func appSQLTargetKeysForLayer(layerID string) []string {
	if layerID == dbLayerIDPostgresPrimary {
		return []string{"postgres", ""}
	}
	return []string{layerID}
}

func sumTargetValues(byTarget map[string]float64, keys ...string) float64 {
	var sum float64
	for _, key := range keys {
		sum += byTarget[key]
	}
	return sum
}

func pickTargetValue(byTarget map[string]float64, keys ...string) float64 {
	for _, key := range keys {
		if v, ok := byTarget[key]; ok && v > 0 {
			return v
		}
	}
	for _, key := range keys {
		if v, ok := byTarget[key]; ok {
			return v
		}
	}
	return 0
}

func mapAppSQLTargetToLayerID(target string, knownReplicas map[string]struct{}) (string, bool) {
	target = strings.TrimSpace(target)
	if target == "" || target == dbLayerIDPostgresPrimary {
		return dbLayerIDPostgresPrimary, true
	}
	if _, ok := knownReplicas[target]; ok {
		return target, true
	}
	return "", false
}

func applyAppSQLMetricsToLayer(
	layer *ComponentLayerMetrics,
	rateByTarget, p50ByTarget, p95ByTarget, p99ByTarget map[string]float64,
	p50SessionByTarget, p95SessionByTarget, p99SessionByTarget map[string]float64,
	sessionTotal float64,
) {
	if layer == nil {
		return
	}
	keys := appSQLTargetKeysForLayer(layer.ID)
	layer.DbQueryRate = sumTargetValues(rateByTarget, keys...)
	layer.P50Latency = pickTargetValue(p50ByTarget, keys...)
	layer.P95Latency = pickTargetValue(p95ByTarget, keys...)
	layer.P99Latency = pickTargetValue(p99ByTarget, keys...)
	layer.P50Latency9m = pickTargetValue(p50SessionByTarget, keys...)
	layer.P95Latency9m = pickTargetValue(p95SessionByTarget, keys...)
	layer.P99Latency9m = pickTargetValue(p99SessionByTarget, keys...)
	layer.DbTotalQueries = sessionTotal
}

func (c *MetricsCollector) enrichDBLayerAppSQL(ctx context.Context, ns string, split *dbReplicaLayerSplit) {
	if c == nil || split == nil || (split.writerPod == "" && len(split.replicaPods) == 0) {
		return
	}

	appSel := appServerPromSelector(ns)
	liveRange := promRateRange()
	rateByTarget := c.queryMultiResult(ctx, dbAppSQLRateByTargetQuery(appSel, liveRange), "target")
	p50ByTarget := dbAppSQLTargetLatencyQuantile(ctx, c, appSel, liveRange, 0.50)
	p95ByTarget := dbAppSQLTargetLatencyQuantile(ctx, c, appSel, liveRange, 0.95)
	p99ByTarget := dbAppSQLTargetLatencyQuantile(ctx, c, appSel, liveRange, 0.99)

	totalByTarget := c.queryMultiResult(ctx, dbAppSQLTotalByTargetQuery(appSel), "target")
	sessionRange := sessionP99PromRange(c.sessionAcc.Elapsed(ns, time.Now()))
	p50SessionByTarget := dbAppSQLTargetLatencyQuantile(ctx, c, appSel, sessionRange, 0.50)
	p95SessionByTarget := dbAppSQLTargetLatencyQuantile(ctx, c, appSel, sessionRange, 0.95)
	p99SessionByTarget := dbAppSQLTargetLatencyQuantile(ctx, c, appSel, sessionRange, 0.99)

	knownReplicas := make(map[string]struct{}, len(split.replicaPods))
	for _, podName := range split.replicaPods {
		knownReplicas[podName] = struct{}{}
	}

	sessionTotalsByLayer := make(map[string]float64)
	countsByLayer := make(map[string]float64)
	for target, count := range totalByTarget {
		layerID, ok := mapAppSQLTargetToLayerID(target, knownReplicas)
		if !ok {
			continue
		}
		countsByLayer[layerID] += count
	}
	for layerID, count := range countsByLayer {
		sessionTotalsByLayer[layerID] = c.sessionAcc.Observe(
			ns,
			dbLayerSessionSeries(seriesSQL, layerID),
			map[string]float64{layerID: count},
		)
	}

	applyAppSQLMetricsToLayer(
		&split.primary,
		rateByTarget, p50ByTarget, p95ByTarget, p99ByTarget,
		p50SessionByTarget, p95SessionByTarget, p99SessionByTarget,
		sessionTotalsByLayer[split.primary.ID],
	)
	for i := range split.replicas {
		layer := &split.replicas[i]
		applyAppSQLMetricsToLayer(
			layer,
			rateByTarget, p50ByTarget, p95ByTarget, p99ByTarget,
			p50SessionByTarget, p95SessionByTarget, p99SessionByTarget,
			sessionTotalsByLayer[layer.ID],
		)
	}
}
