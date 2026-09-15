package metrics

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/ywacch/cloud9/pkg/deployer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	dbReplicaAppLabel        = "postgres-replica"
	dbReplicaStatefulSetName = "postgres-replica"
	dbLayerRolePrimary       = "primary"
	dbLayerRoleReplica       = "replica"
	dbLayerEnginePostgreSQL  = "postgresql"
	dbLayerIDPostgresPrimary = "postgres"
	dbReplicaPodSelector     = "postgres-replica.*"
	dbPrimaryPodSelector     = "postgres.*"
	dbPrimaryContainerName   = "postgres"
	dbReplicaContainerName   = "postgres-replica"
	// postgresExporterReplayLagMetric is from deployer custom query block replication_replay_lag.
	// postgres-exporter v0.16.0 names custom metrics {block}_{column} without a pg_ prefix.
	postgresExporterReplayLagMetric      = "replication_replay_lag_replay_lag_seconds"
	postgresExporterReplayLagBytesMetric = "replication_replay_lag_replay_lag_bytes"
	// postgresExporterReplicaLagFallbackMetric is the built-in replica lag gauge on standby pods.
	postgresExporterReplicaLagFallbackMetric = "pg_replication_lag_seconds"
)

// dbReplicaLayerSplit holds per-instance Postgres layers when read replicas are deployed.
type dbReplicaLayerSplit struct {
	replicaPods        []string
	ready              map[string]bool
	connectedOnPrimary map[string]bool
	counts             BackendReplicas
	primary            ComponentLayerMetrics
	replicas           []ComponentLayerMetrics
}

// listDBReplicaPods returns replica pod names, readiness, and StatefulSet desired/ready counts.
func (c *MetricsCollector) listDBReplicaPods(ctx context.Context, ns string) (names []string, ready map[string]bool, counts BackendReplicas) {
	ready = make(map[string]bool)
	if c == nil || c.clientset == nil {
		return nil, ready, counts
	}
	stsList, err := c.clientset.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s", dbReplicaAppLabel),
	})
	if err == nil {
		for _, sts := range stsList.Items {
			if sts.Spec.Replicas != nil {
				counts.Desired += *sts.Spec.Replicas
			}
		}
	}
	list, err := c.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s,deployment=%s", dbReplicaAppLabel, ns),
	})
	if err != nil {
		return nil, ready, counts
	}
	for _, p := range list.Items {
		isReady := podReadyStatus(p)
		ready[p.Name] = isReady
		names = append(names, p.Name)
		if isReady {
			counts.Ready++
		}
	}
	sort.Strings(names)
	return names, ready, counts
}

// postgresPrimaryReplayLagByAppQuery reads replay lag seconds from the primary postgres-exporter
// custom query (pg_stat_replication.replay_lag), keyed by application_name (= replica pod hostname).
func postgresPrimaryReplayLagByAppQuery(ns string) string {
	return postgresPrimaryReplicationMetricByAppQuery(ns, postgresExporterReplayLagMetric)
}

func postgresPrimaryReplayLagBytesByAppQuery(ns string) string {
	return postgresPrimaryReplicationMetricByAppQuery(ns, postgresExporterReplayLagBytesMetric)
}

func postgresPrimaryReplicationMetricByAppQuery(ns, metric string) string {
	return fmt.Sprintf(
		`max by (application_name) (%s{namespace=%q,pod=~%q,pod!~%q})`,
		metric, ns, dbPrimaryPodSelector, dbReplicaPodSelector,
	)
}

func postgresPrimarySyncStateByAppQuery(ns string) string {
	return fmt.Sprintf(
		`max by (application_name, sync_state) (%s{namespace=%q,pod=~%q,pod!~%q})`,
		postgresExporterReplayLagMetric, ns, dbPrimaryPodSelector, dbReplicaPodSelector,
	)
}

// postgresReplicaLagFallbackByPodQuery is used when the primary custom series is absent.
func postgresReplicaLagFallbackByPodQuery(ns string) string {
	return fmt.Sprintf(
		`max by (pod) (%s{namespace=%q,pod=~%q})`,
		postgresExporterReplicaLagFallbackMetric, ns, dbReplicaPodSelector,
	)
}

func postgresXactRateByPodQuery(ns string) string {
	return fmt.Sprintf(
		`sum by (pod) (rate(pg_stat_database_xact_commit{namespace=%q,datname!=""}%s) + rate(pg_stat_database_xact_rollback{namespace=%q,datname!=""}%s))`,
		ns, promRateRange(), ns, promRateRange(),
	)
}

func postgresXactTotalByPodQuery(ns string) string {
	return fmt.Sprintf(
		`sum by (pod) (pg_stat_database_xact_commit{namespace=%q,datname!=""} + pg_stat_database_xact_rollback{namespace=%q,datname!=""})`,
		ns, ns,
	)
}

func postgresActiveConnectionsByPodQuery(ns string) string {
	return fmt.Sprintf(
		`sum by (pod) (pg_stat_activity_count{namespace=%q,state="active"})`,
		ns,
	)
}

func postgresNumBackendsByPodQuery(ns string) string {
	return fmt.Sprintf(
		`sum by (pod) (pg_stat_database_numbackends{namespace=%q,datname!=""})`,
		ns,
	)
}

func dbLayerSessionSeries(baseSeries, layerID string) string {
	return baseSeries + "." + layerID
}

func mapPostgresLayerPodValues(byPod map[string]float64) map[string]float64 {
	out := make(map[string]float64)
	replicaRE, err := regexp.Compile("^" + dbReplicaPodSelector + "$")
	if err != nil {
		return out
	}
	for pod, v := range byPod {
		if replicaRE.MatchString(pod) {
			out[pod] = v
			continue
		}
		out[dbLayerIDPostgresPrimary] += v
	}
	return out
}

func mapPostgresLayerRequestRates(rpsByPod map[string]float64) map[string]float64 {
	return mapPostgresLayerPodValues(rpsByPod)
}

func (c *MetricsCollector) dbLayerRequestRateByPod(ctx context.Context, ns string) map[string]float64 {
	if c == nil || c.prometheusURL == "" {
		return nil
	}
	return mapPostgresLayerRequestRates(c.queryMultiResult(ctx, postgresXactRateByPodQuery(ns), "pod"))
}

func (c *MetricsCollector) postgresConnectionsByPod(ctx context.Context, ns string) map[string]float64 {
	if c == nil || c.prometheusURL == "" {
		return nil
	}
	byPod := c.queryMultiResult(ctx, postgresActiveConnectionsByPodQuery(ns), "pod")
	if len(byPod) == 0 {
		byPod = c.queryMultiResult(ctx, postgresNumBackendsByPodQuery(ns), "pod")
	}
	return byPod
}

func replicaReadOnlyStorageIO(cluster *StorageIO, readRate, readTotal9m float64) *StorageIO {
	io := &StorageIO{
		Unit:        "rows/s",
		Source:      "postgres_exporter",
		ReadRate:    readRate,
		ReadTotal9m: readTotal9m,
		ReadLabel:   "Rows scanned/s",
	}
	if cluster == nil {
		return io
	}
	if cluster.Unit != "" {
		io.Unit = cluster.Unit
	}
	if cluster.Source != "" {
		io.Source = cluster.Source
	}
	if cluster.ReadLabel != "" {
		io.ReadLabel = cluster.ReadLabel
	}
	return io
}

func (c *MetricsCollector) enrichDBLayerEngineMetrics(ctx context.Context, ns string, split *dbReplicaLayerSplit, clusterStorageIO *StorageIO) {
	if c == nil || split == nil || len(split.replicaPods) == 0 {
		return
	}
	held := c.postgresSessionHeld(ns)

	xactByPod := c.queryMultiResult(ctx, postgresXactTotalByPodQuery(ns), "pod")
	xactByLayer := mapPostgresLayerPodValues(xactByPod)
	connByPod := c.postgresConnectionsByPod(ctx, ns)
	connByLayer := mapPostgresLayerPodValues(connByPod)

	if !held {
		if count, ok := xactByLayer[dbLayerIDPostgresPrimary]; ok {
			split.primary.TotalRequests = c.sessionAcc.ObserveDeltas(
				ns,
				dbLayerSessionSeries(seriesPgXact, dbLayerIDPostgresPrimary),
				map[string]float64{dbLayerIDPostgresPrimary: count},
			)
		}
	}
	if conn, ok := connByLayer[dbLayerIDPostgresPrimary]; ok {
		split.primary.DbActiveConnections = conn
	}
	split.primary.StorageIO = cloneStorageIO(clusterStorageIO)

	for i := range split.replicas {
		layer := &split.replicas[i]
		podName := layer.ID

		if !held {
			if count, ok := xactByPod[podName]; ok {
				layer.TotalRequests = c.sessionAcc.ObserveDeltas(
					ns,
					dbLayerSessionSeries(seriesPgXact, podName),
					map[string]float64{podName: count},
				)
			}
		}
		if conn, ok := connByPod[podName]; ok {
			layer.DbActiveConnections = conn
		}

		readRate, _ := c.query(ctx, postgresReplicaUserTableReadRateQuery(ns, podName))
		var readTotal9m float64
		if !held {
			if total, ok, _ := c.queryScalar(ctx, postgresReplicaUserTableReadTotalQuery(ns, podName)); ok {
				readTotal9m = c.sessionAcc.ObserveDeltas(
					ns,
					dbLayerSessionSeries(seriesPgRead, podName),
					map[string]float64{podName: total},
				)
			}
		}
		layer.StorageIO = replicaReadOnlyStorageIO(clusterStorageIO, readRate, readTotal9m)
	}
}

func (c *MetricsCollector) replicaLagBytesByPod(ctx context.Context, ns string, replicaPods []string) map[string]int64 {
	out := make(map[string]int64, len(replicaPods))
	if c == nil || c.prometheusURL == "" || len(replicaPods) == 0 {
		return out
	}
	for appName, lag := range c.queryMultiResult(ctx, postgresPrimaryReplayLagBytesByAppQuery(ns), "application_name") {
		out[appName] = int64(lag)
	}
	return out
}

func (c *MetricsCollector) replicaSyncStateByPod(ctx context.Context, ns string, replicaPods []string) map[string]string {
	out := make(map[string]string, len(replicaPods))
	if c == nil || c.prometheusURL == "" || len(replicaPods) == 0 {
		return out
	}
	return c.queryMultiLabel(ctx, postgresPrimarySyncStateByAppQuery(ns), "application_name", "sync_state")
}

func applyReplicaPromotionPreview(layers []ComponentLayerMetrics, connected map[string]bool, syncByPod map[string]string, lagBytesByPod map[string]int64) {
	var candidates []deployer.PromotionCandidate
	for i := range layers {
		if layers[i].Role != dbLayerRoleReplica {
			continue
		}
		podName := layers[i].ID
		if !connected[podName] {
			layers[i].SyncState = "async"
			continue
		}
		syncState := syncByPod[podName]
		if syncState == "" {
			syncState = "async"
		}
		layers[i].SyncState = syncState
		if lagBytes, ok := lagBytesByPod[podName]; ok {
			layers[i].LagBytes = &lagBytes
		}
		candidates = append(candidates, deployer.PromotionCandidate{
			PodName:   podName,
			SyncState: syncState,
			LagBytes:  lagBytesByPod[podName],
		})
	}
	picked, err := deployer.PickCandidate(candidates, deployer.PreferMostCurrent)
	if err != nil {
		return
	}
	for i := range layers {
		if layers[i].Role == dbLayerRoleReplica && layers[i].ID == picked.PodName {
			layers[i].PromotionCandidate = true
		}
	}
}

func (c *MetricsCollector) replicaLagByPod(ctx context.Context, ns string, replicaPods []string) map[string]float64 {
	out := make(map[string]float64, len(replicaPods))
	if c == nil || c.prometheusURL == "" || len(replicaPods) == 0 {
		return out
	}
	for appName, lag := range c.queryMultiResult(ctx, postgresPrimaryReplayLagByAppQuery(ns), "application_name") {
		out[appName] = lag
	}
	fallback := c.queryMultiResult(ctx, postgresReplicaLagFallbackByPodQuery(ns), "pod")
	for _, podName := range replicaPods {
		if _, ok := out[podName]; ok {
			continue
		}
		if lag, ok := fallback[podName]; ok {
			out[podName] = lag
		}
	}
	return out
}

// replicaConnectedOnPrimaryByPod reports which replica pods have a pg_stat_replication row
// on the primary (application_name = pod hostname). Fallback standby-side lag is ignored.
func (c *MetricsCollector) replicaConnectedOnPrimaryByPod(ctx context.Context, ns string, replicaPods []string) map[string]bool {
	out := make(map[string]bool, len(replicaPods))
	for _, podName := range replicaPods {
		out[podName] = false
	}
	if c == nil || c.prometheusURL == "" || len(replicaPods) == 0 {
		return out
	}
	for appName := range c.queryMultiResult(ctx, postgresPrimaryReplayLagByAppQuery(ns), "application_name") {
		if _, ok := out[appName]; ok {
			out[appName] = true
		}
	}
	return out
}

func (c *MetricsCollector) buildDBPrimaryLayer(ctx context.Context, ns string) ComponentLayerMetrics {
	layer := ComponentLayerMetrics{
		ID:     dbLayerIDPostgresPrimary,
		Role:   dbLayerRolePrimary,
		Engine: dbLayerEnginePostgreSQL,
	}
	if c == nil {
		return layer
	}
	cpuRes := c.queryContainerResourceCPUExcluding(ctx, ns, dbPrimaryPodSelector, dbReplicaPodSelector, dbPrimaryContainerName)
	memRes := c.queryContainerResourceMemoryExcluding(ctx, ns, dbPrimaryPodSelector, dbReplicaPodSelector, dbPrimaryContainerName)
	layer.CPUUsage = cpuRes.Usage
	layer.CPUIsEstimated = cpuRes.Estimated
	layer.CPUSource = cpuRes.Source
	layer.MemoryBytes = int64(memRes.Usage)
	layer.MemIsEstimated = memRes.Estimated
	layer.MemSource = memRes.Source
	layer.CPULimit = c.kubeLimitCPUExcluding(ctx, ns, dbPrimaryPodSelector, dbReplicaPodSelector, dbPrimaryContainerName)
	layer.MemoryLimit = c.kubeLimitMemExcluding(ctx, ns, dbPrimaryPodSelector, dbReplicaPodSelector, dbPrimaryContainerName)
	return layer
}

func replicationLagSecondsPtr(seconds float64) *float64 {
	v := seconds
	return &v
}

func (c *MetricsCollector) buildDBReplicaLayer(ctx context.Context, ns, podName string, lagSeconds *float64) ComponentLayerMetrics {
	layer := ComponentLayerMetrics{
		ID:                    podName,
		Role:                  dbLayerRoleReplica,
		Engine:                dbLayerEnginePostgreSQL,
		ReplicationLagSeconds: lagSeconds,
	}
	if c == nil {
		return layer
	}
	podRe := fmt.Sprintf("^%s$", podName)
	cpuRes := c.queryContainerResourceCPU(ctx, ns, podRe, dbReplicaContainerName)
	memRes := c.queryContainerResourceMemory(ctx, ns, podRe, dbReplicaContainerName)
	layer.CPUUsage = cpuRes.Usage
	layer.CPUIsEstimated = cpuRes.Estimated
	layer.CPUSource = cpuRes.Source
	layer.MemoryBytes = int64(memRes.Usage)
	layer.MemIsEstimated = memRes.Estimated
	layer.MemSource = memRes.Source
	layer.CPULimit = c.kubeLimitCPU(ctx, ns, podRe, dbReplicaContainerName)
	layer.MemoryLimit = c.kubeLimitMem(ctx, ns, podRe, dbReplicaContainerName)
	return layer
}

// collectDBReplicaLayers builds primary + per-replica instance layers when postgres-replica pods exist.
func (c *MetricsCollector) collectDBReplicaLayers(ctx context.Context, ns string, clusterStorageIO *StorageIO) dbReplicaLayerSplit {
	out := dbReplicaLayerSplit{ready: make(map[string]bool)}
	if c == nil {
		return out
	}
	names, ready, counts := c.listDBReplicaPods(ctx, ns)
	out.replicaPods = names
	out.ready = ready
	out.counts = counts
	if len(names) == 0 {
		return out
	}

	out.primary = c.buildDBPrimaryLayer(ctx, ns)
	lagByPod := c.replicaLagByPod(ctx, ns, names)
	lagBytesByPod := c.replicaLagBytesByPod(ctx, ns, names)
	syncByPod := c.replicaSyncStateByPod(ctx, ns, names)
	out.connectedOnPrimary = c.replicaConnectedOnPrimaryByPod(ctx, ns, names)
	rpsByLayer := c.dbLayerRequestRateByPod(ctx, ns)
	out.primary.RequestRate = rpsByLayer[dbLayerIDPostgresPrimary]
	out.replicas = make([]ComponentLayerMetrics, 0, len(names))
	for _, podName := range names {
		var lag *float64
		if seconds, ok := lagByPod[podName]; ok {
			lag = replicationLagSecondsPtr(seconds)
		}
		replicaLayer := c.buildDBReplicaLayer(ctx, ns, podName, lag)
		replicaLayer.RequestRate = rpsByLayer[podName]
		out.replicas = append(out.replicas, replicaLayer)
	}
	applyReplicaPromotionPreview(out.replicas, out.connectedOnPrimary, syncByPod, lagBytesByPod)
	c.enrichDBLayerEngineMetrics(ctx, ns, &out, clusterStorageIO)
	c.enrichDBLayerAppSQL(ctx, ns, &out)
	return out
}

// applyDBReplicaLayers attaches db instance layers and replica StatefulSet rollout counts.
func applyDBReplicaLayers(cm *ComponentMetrics, split dbReplicaLayerSplit) {
	if cm == nil || len(split.replicaPods) == 0 {
		return
	}
	layers := make([]ComponentLayerMetrics, 0, 1+len(split.replicas))
	layers = append(layers, split.primary)
	layers = append(layers, split.replicas...)
	cm.Layers = layers
	br := split.counts
	cm.BackendReplicas = &br
}

// syncDBReplicaLayerRestarts copies restart counts from runtimeHealth pod rows onto matching layers.
func syncDBReplicaLayerRestarts(cm *ComponentMetrics) {
	if cm == nil || len(cm.Layers) == 0 || cm.RuntimeHealth == nil {
		return
	}
	for i := range cm.Layers {
		layer := &cm.Layers[i]
		prefix := "postgres"
		if layer.Role == dbLayerRoleReplica {
			prefix = layer.ID
		}
		for _, pod := range cm.RuntimeHealth.Pods {
			if !strings.HasPrefix(pod.Name, prefix) {
				continue
			}
			if pod.RestartCount > layer.RestartCount {
				layer.RestartCount = pod.RestartCount
			}
		}
	}
}
