package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

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
	// postgresExporterWalLsnBytesMetric is primary pg_current_wal_lsn() as bytes from 0/0.
	postgresExporterWalLsnBytesMetric = "wal_lsn_bytes"
	// postgresExporterReplayLsnBytesMetric is standby pg_last_wal_replay_lsn() as bytes from 0/0.
	postgresExporterReplayLsnBytesMetric = "wal_replay_lsn_bytes"
)

type primaryReplicationSnapshot struct {
	connected   map[string]bool
	lagSeconds  map[string]float64
	lagBytes    map[string]int64
	syncState   map[string]string
	writerLSN   float64 // pg_wal_lsn_diff(current, 0/0); 0 if unknown
	writerLSNOk bool
}

type replSQLCacheEntry struct {
	snap      primaryReplicationSnapshot
	expiresAt time.Time
}

func newPrimaryReplicationSnapshot(standbys []string) primaryReplicationSnapshot {
	snap := primaryReplicationSnapshot{
		connected:  make(map[string]bool, len(standbys)),
		lagSeconds: make(map[string]float64),
		lagBytes:   make(map[string]int64),
		syncState:  make(map[string]string),
	}
	for _, podName := range standbys {
		snap.connected[podName] = false
	}
	return snap
}

func copyPrimaryReplicationSnapshot(snap primaryReplicationSnapshot) primaryReplicationSnapshot {
	out := primaryReplicationSnapshot{
		connected:   make(map[string]bool, len(snap.connected)),
		lagSeconds:  make(map[string]float64, len(snap.lagSeconds)),
		lagBytes:    make(map[string]int64, len(snap.lagBytes)),
		syncState:   make(map[string]string, len(snap.syncState)),
		writerLSN:   snap.writerLSN,
		writerLSNOk: snap.writerLSNOk,
	}
	for k, v := range snap.connected {
		out.connected[k] = v
	}
	for k, v := range snap.lagSeconds {
		out.lagSeconds[k] = v
	}
	for k, v := range snap.lagBytes {
		out.lagBytes[k] = v
	}
	for k, v := range snap.syncState {
		out.syncState[k] = v
	}
	return out
}

func parseReplicationStateRows(stdout string) (connected map[string]bool, lagSeconds map[string]float64, lagBytes map[string]int64, syncState map[string]string) {
	connected = make(map[string]bool)
	lagSeconds = make(map[string]float64)
	lagBytes = make(map[string]int64)
	syncState = make(map[string]string)
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "|")
		if len(fields) != 5 {
			slog.Debug("skip malformed pg_stat_replication row", "line", line)
			continue
		}
		app := strings.TrimSpace(fields[0])
		sync := strings.TrimSpace(fields[2])
		secs, err := strconv.ParseFloat(strings.TrimSpace(fields[3]), 64)
		if err != nil {
			slog.Debug("skip pg_stat_replication replay_lag seconds", "line", line, "error", err)
			continue
		}
		bytes, err := strconv.ParseInt(strings.TrimSpace(fields[4]), 10, 64)
		if err != nil {
			slog.Debug("skip pg_stat_replication lag bytes", "line", line, "error", err)
			continue
		}
		connected[app] = true
		syncState[app] = sync
		lagSeconds[app] = secs
		lagBytes[app] = bytes
	}
	return connected, lagSeconds, lagBytes, syncState
}

func (c *MetricsCollector) replicationAdmin(ns, pod, container string) deployer.PostgresAdmin {
	if c != nil && c.replicationAdminForTest != nil {
		return c.replicationAdminForTest(ns, pod, container)
	}
	return deployer.NewPostgresPodAdmin(c.clientset, ns, pod, container)
}

func (c *MetricsCollector) loadWriterReplicationSQL(ctx context.Context, ns, writerPod string, standbys []string) (primaryReplicationSnapshot, error) {
	snap := newPrimaryReplicationSnapshot(standbys)
	if c == nil || c.clientset == nil || writerPod == "" {
		return snap, nil
	}
	admin := c.replicationAdmin(ns, writerPod, dbReplicaContainerName)
	stdout, err := admin.ExecSQL(ctx, deployer.ReplicationStateSQL)
	if err != nil {
		return snap, err
	}
	parsedConnected, parsedLagSeconds, parsedLagBytes, parsedSyncState := parseReplicationStateRows(stdout)
	for app, ok := range parsedConnected {
		if ok {
			snap.connected[app] = true
		}
	}
	for k, v := range parsedLagSeconds {
		snap.lagSeconds[k] = v
	}
	for k, v := range parsedLagBytes {
		snap.lagBytes[k] = v
	}
	for k, v := range parsedSyncState {
		snap.syncState[k] = v
	}
	lsnOut, err := admin.ExecSQL(ctx, deployer.CurrentWalLSNBytesSQL)
	if err == nil {
		lsnStr := strings.TrimSpace(lsnOut)
		if lsnStr != "" {
			if v, err := strconv.ParseFloat(lsnStr, 64); err == nil {
				snap.writerLSN = v
				snap.writerLSNOk = true
			}
		}
	}
	return snap, nil
}

func (c *MetricsCollector) cachedWriterReplicationSQL(ctx context.Context, ns, writerPod string, standbys []string) primaryReplicationSnapshot {
	empty := newPrimaryReplicationSnapshot(standbys)
	if c == nil {
		return empty
	}

	c.replSQLMu.Lock()
	if c.replSQLCache == nil {
		c.replSQLCache = make(map[string]replSQLCacheEntry)
	}
	if entry, ok := c.replSQLCache[ns]; ok && time.Now().Before(entry.expiresAt) {
		snap := copyPrimaryReplicationSnapshot(entry.snap)
		c.replSQLMu.Unlock()
		return snap
	}
	c.replSQLMu.Unlock()

	result, err, _ := c.replSQLFlight.Do(ns, func() (interface{}, error) {
		execCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return c.loadWriterReplicationSQL(execCtx, ns, writerPod, standbys)
	})

	if err != nil {
		c.replSQLMu.Lock()
		if entry, ok := c.replSQLCache[ns]; ok {
			snap := copyPrimaryReplicationSnapshot(entry.snap)
			c.replSQLMu.Unlock()
			return snap
		}
		c.replSQLMu.Unlock()
		return empty
	}

	snap := result.(primaryReplicationSnapshot)
	c.replSQLMu.Lock()
	c.replSQLCache[ns] = replSQLCacheEntry{
		snap:      copyPrimaryReplicationSnapshot(snap),
		expiresAt: time.Now().Add(5 * time.Second),
	}
	c.replSQLMu.Unlock()
	return snap
}

func (c *MetricsCollector) loadPrimaryReplicationSnapshot(ctx context.Context, ns, writerPod string, standbys []string) primaryReplicationSnapshot {
	snap := newPrimaryReplicationSnapshot(standbys)
	if c == nil || c.prometheusURL == "" {
		return snap
	}

	lagSeconds := c.queryMultiResult(ctx, postgresPrimaryReplayLagByAppQuery(ns, writerPod), "application_name")
	for appName, lag := range lagSeconds {
		snap.lagSeconds[appName] = lag
		if _, ok := snap.connected[appName]; ok {
			snap.connected[appName] = true
		}
	}

	if len(lagSeconds) > 0 {
		for appName, lag := range c.queryMultiResult(ctx, postgresPrimaryReplayLagBytesByAppQuery(ns, writerPod), "application_name") {
			snap.lagBytes[appName] = int64(lag)
		}
		snap.syncState = c.queryMultiLabel(ctx, postgresPrimarySyncStateByAppQuery(ns, writerPod), "application_name", "sync_state")
		if writerLSN, present, _ := c.queryScalar(ctx, postgresPrimaryWalLsnBytesQuery(ns, writerPod)); present {
			snap.writerLSN = writerLSN
			snap.writerLSNOk = true
		}
		return snap
	}

	if writerPod != "" && c.postgresWriterContainer(ctx, ns, writerPod) == dbReplicaContainerName {
		return c.cachedWriterReplicationSQL(ctx, ns, writerPod, standbys)
	}

	return snap
}

// dbReplicaLayerSplit holds per-instance Postgres layers when read replicas are deployed.
type dbReplicaLayerSplit struct {
	writerPod          string
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
func postgresPrimaryReplayLagByAppQuery(ns, writerPod string) string {
	return postgresPrimaryReplicationMetricByAppQuery(ns, postgresExporterReplayLagMetric, writerPod)
}

func postgresPrimaryReplayLagBytesByAppQuery(ns, writerPod string) string {
	return postgresPrimaryReplicationMetricByAppQuery(ns, postgresExporterReplayLagBytesMetric, writerPod)
}

func postgresPrimaryReplicationMetricByAppQuery(ns, metric, writerPod string) string {
	return fmt.Sprintf(
		`max by (application_name) (%s{namespace=%q,pod="%s"})`,
		metric, ns, writerPod,
	)
}

func postgresPrimarySyncStateByAppQuery(ns, writerPod string) string {
	return fmt.Sprintf(
		`max by (application_name, sync_state) (%s{namespace=%q,pod="%s"})`,
		postgresExporterReplayLagMetric, ns, writerPod,
	)
}

// postgresReplicaLagFallbackByPodQuery is used when the primary custom series is absent.
func postgresReplicaLagFallbackByPodQuery(ns string) string {
	return fmt.Sprintf(
		`max by (pod) (%s{namespace=%q,pod=~%q})`,
		postgresExporterReplicaLagFallbackMetric, ns, dbReplicaPodSelector,
	)
}

func postgresPrimaryWalLsnBytesQuery(ns, writerPod string) string {
	return fmt.Sprintf(`max(%s{namespace=%q,pod=%q})`, postgresExporterWalLsnBytesMetric, ns, writerPod)
}

func postgresReplicaReplayLsnBytesByPodQuery(ns string) string {
	return fmt.Sprintf(
		`max by (pod) (%s{namespace=%q,pod=~%q})`,
		postgresExporterReplayLsnBytesMetric, ns, dbReplicaPodSelector,
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

func mapPostgresLayerPodValues(byPod map[string]float64, writerPod string) map[string]float64 {
	out := make(map[string]float64)
	replicaRE, err := regexp.Compile("^" + dbReplicaPodSelector + "$")
	if err != nil {
		return out
	}
	for pod, v := range byPod {
		if writerPod != "" && pod == writerPod {
			out[dbLayerIDPostgresPrimary] += v
			continue
		}
		if replicaRE.MatchString(pod) {
			out[pod] = v
			continue
		}
		out[dbLayerIDPostgresPrimary] += v
	}
	return out
}

func mapPostgresLayerRequestRates(rpsByPod map[string]float64, writerPod string) map[string]float64 {
	return mapPostgresLayerPodValues(rpsByPod, writerPod)
}

func (c *MetricsCollector) dbLayerRequestRateByPod(ctx context.Context, ns, writerPod string) map[string]float64 {
	if c == nil || c.prometheusURL == "" {
		return nil
	}
	return mapPostgresLayerRequestRates(c.queryMultiResult(ctx, postgresXactRateByPodQuery(ns), "pod"), writerPod)
}

func postgresClusterNumBackendsQuery(ns string) string {
	return fmt.Sprintf(
		`sum(pg_stat_database_numbackends{namespace=%q,datname!=""})`,
		ns,
	)
}

// postgresClusterBackends is the live client session count on postgres (pg_stat_database.numbackends).
func (c *MetricsCollector) postgresClusterBackends(ctx context.Context, ns string) float64 {
	if c == nil || c.prometheusURL == "" {
		return 0
	}
	v, _ := c.query(ctx, postgresClusterNumBackendsQuery(ns))
	if v > 0 {
		return v
	}
	v, _ = c.query(ctx, fmt.Sprintf(
		`sum(pg_stat_database_numbackends{namespace=%q})`,
		ns,
	))
	return v
}

func (c *MetricsCollector) postgresConnectionsByPod(ctx context.Context, ns string) map[string]float64 {
	if c == nil || c.prometheusURL == "" {
		return nil
	}
	byPod := c.queryMultiResult(ctx, postgresNumBackendsByPodQuery(ns), "pod")
	if len(byPod) > 0 {
		return byPod
	}
	return c.queryMultiResult(ctx, postgresActiveConnectionsByPodQuery(ns), "pod")
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
	if c == nil || split == nil || (split.writerPod == "" && len(split.replicaPods) == 0) {
		return
	}
	held := c.postgresSessionHeld(ns)

	xactByPod := c.queryMultiResult(ctx, postgresXactTotalByPodQuery(ns), "pod")
	xactByLayer := mapPostgresLayerPodValues(xactByPod, split.writerPod)
	connByPod := c.postgresConnectionsByPod(ctx, ns)
	connByLayer := mapPostgresLayerPodValues(connByPod, split.writerPod)

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

func (c *MetricsCollector) replicaLagBytesByPod(ctx context.Context, ns, writerPod string, replicaPods []string) map[string]int64 {
	out := make(map[string]int64, len(replicaPods))
	if c == nil || c.prometheusURL == "" || writerPod == "" || len(replicaPods) == 0 {
		return out
	}
	for appName, lag := range c.queryMultiResult(ctx, postgresPrimaryReplayLagBytesByAppQuery(ns, writerPod), "application_name") {
		out[appName] = int64(lag)
	}
	return out
}

func walLsnDiffBytes(primary, replay float64) int64 {
	diff := int64(primary) - int64(replay)
	if diff < 0 {
		return 0
	}
	return diff
}

func lagBytesPtr(bytes int64) *int64 {
	v := bytes
	return &v
}

// replicaPartitionLagBytesByPod is primary WAL LSN minus frozen standby replay LSN.
// Used only when the standby has no pg_stat_replication row (TCP stall / partition).
func (c *MetricsCollector) replicaPartitionLagBytesByPod(ctx context.Context, ns, writerPod string, replicaPods []string, connected map[string]bool, writerLSN float64, writerLSNOk bool) map[string]int64 {
	out := make(map[string]int64, len(replicaPods))
	if c == nil || c.prometheusURL == "" || writerPod == "" || len(replicaPods) == 0 {
		return out
	}
	hasDisconnected := false
	for _, podName := range replicaPods {
		if !connected[podName] {
			hasDisconnected = true
			break
		}
	}
	if !hasDisconnected {
		return out
	}
	var primaryLSN float64
	var present bool
	if writerLSNOk {
		primaryLSN = writerLSN
		present = true
	} else {
		primaryLSN, present, _ = c.queryScalar(ctx, postgresPrimaryWalLsnBytesQuery(ns, writerPod))
	}
	if !present {
		return out
	}
	replayByPod := c.queryMultiResult(ctx, postgresReplicaReplayLsnBytesByPodQuery(ns), "pod")
	for _, podName := range replicaPods {
		if connected[podName] {
			continue
		}
		replay, ok := replayByPod[podName]
		if !ok {
			continue
		}
		out[podName] = walLsnDiffBytes(primaryLSN, replay)
	}
	return out
}

func (c *MetricsCollector) replicaSyncStateByPod(ctx context.Context, ns, writerPod string, replicaPods []string) map[string]string {
	out := make(map[string]string, len(replicaPods))
	if c == nil || c.prometheusURL == "" || writerPod == "" || len(replicaPods) == 0 {
		return out
	}
	return c.queryMultiLabel(ctx, postgresPrimarySyncStateByAppQuery(ns, writerPod), "application_name", "sync_state")
}

func applyReplicaPromotionPreview(layers []ComponentLayerMetrics, connected map[string]bool, syncByPod map[string]string, lagBytesByPod map[string]int64) {
	var candidates []deployer.PromotionCandidate
	for i := range layers {
		if layers[i].Role != dbLayerRoleReplica {
			continue
		}
		podName := layers[i].ID
		if !connected[podName] {
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

func (c *MetricsCollector) replicaLagByPod(ctx context.Context, ns, writerPod string, replicaPods []string) map[string]float64 {
	out := make(map[string]float64, len(replicaPods))
	if c == nil || c.prometheusURL == "" || len(replicaPods) == 0 {
		return out
	}
	if writerPod != "" {
		for appName, lag := range c.queryMultiResult(ctx, postgresPrimaryReplayLagByAppQuery(ns, writerPod), "application_name") {
			out[appName] = lag
		}
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
func (c *MetricsCollector) replicaConnectedOnPrimaryByPod(ctx context.Context, ns, writerPod string, replicaPods []string) map[string]bool {
	out := make(map[string]bool, len(replicaPods))
	for _, podName := range replicaPods {
		out[podName] = false
	}
	if c == nil || c.prometheusURL == "" || writerPod == "" || len(replicaPods) == 0 {
		return out
	}
	for appName := range c.queryMultiResult(ctx, postgresPrimaryReplayLagByAppQuery(ns, writerPod), "application_name") {
		if _, ok := out[appName]; ok {
			out[appName] = true
		}
	}
	return out
}

func (c *MetricsCollector) postgresWriterContainer(ctx context.Context, ns, writerPod string) string {
	if c == nil || c.clientset == nil || writerPod == "" {
		return dbPrimaryContainerName
	}
	pod, err := c.clientset.CoreV1().Pods(ns).Get(ctx, writerPod, metav1.GetOptions{})
	if err != nil {
		return dbPrimaryContainerName
	}
	if pod.Labels["app"] == dbReplicaAppLabel {
		return dbReplicaContainerName
	}
	return dbPrimaryContainerName
}

func (c *MetricsCollector) buildDBPrimaryLayer(ctx context.Context, ns, writerPod string) ComponentLayerMetrics {
	layer := ComponentLayerMetrics{
		ID:     dbLayerIDPostgresPrimary,
		Role:   dbLayerRolePrimary,
		Engine: dbLayerEnginePostgreSQL,
	}
	if c == nil {
		return layer
	}
	podRe := dbPrimaryPodSelector
	excludePodRe := dbReplicaPodSelector
	container := dbPrimaryContainerName
	if writerPod != "" {
		podRe = fmt.Sprintf("^%s$", writerPod)
		excludePodRe = ""
		container = c.postgresWriterContainer(ctx, ns, writerPod)
	}
	cpuRes := c.queryContainerResourceCPUExcluding(ctx, ns, podRe, excludePodRe, container)
	memRes := c.queryContainerResourceMemoryExcluding(ctx, ns, podRe, excludePodRe, container)
	layer.CPUUsage = cpuRes.Usage
	layer.CPUIsEstimated = cpuRes.Estimated
	layer.CPUSource = cpuRes.Source
	layer.MemoryBytes = int64(memRes.Usage)
	layer.MemIsEstimated = memRes.Estimated
	layer.MemSource = memRes.Source
	layer.CPULimit = c.kubeLimitCPUExcluding(ctx, ns, podRe, excludePodRe, container)
	layer.MemoryLimit = c.kubeLimitMemExcluding(ctx, ns, podRe, excludePodRe, container)
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

// collectDBReplicaLayers builds primary + per-standby instance layers from live write routing.
func (c *MetricsCollector) collectDBReplicaLayers(ctx context.Context, ns string, clusterStorageIO *StorageIO) dbReplicaLayerSplit {
	ctx = withPromCaller(ctx, "db")
	out := dbReplicaLayerSplit{ready: make(map[string]bool)}
	if c == nil {
		return out
	}

	writerPod := ""
	if c.clientset != nil {
		writerPod, _ = deployer.PostgresWritePod(ctx, c.clientset, ns)
	}
	out.writerPod = writerPod

	names, ready, counts := c.listDBReplicaPods(ctx, ns)
	var standbys []string
	standbyReady := make(map[string]bool)
	var readyCount int32
	for _, name := range names {
		if name == writerPod {
			continue
		}
		standbys = append(standbys, name)
		standbyReady[name] = ready[name]
		if ready[name] {
			readyCount++
		}
	}
	out.replicaPods = standbys
	out.ready = standbyReady
	out.counts = counts
	out.counts.Ready = readyCount

	if writerPod == "" && len(standbys) == 0 {
		return out
	}

	out.primary = c.buildDBPrimaryLayer(ctx, ns, writerPod)
	snap := c.loadPrimaryReplicationSnapshot(ctx, ns, writerPod, standbys)
	out.connectedOnPrimary = make(map[string]bool, len(standbys))
	for _, podName := range standbys {
		out.connectedOnPrimary[podName] = snap.connected[podName]
	}
	rpsByLayer := c.dbLayerRequestRateByPod(ctx, ns, writerPod)
	out.primary.RequestRate = rpsByLayer[dbLayerIDPostgresPrimary]
	out.replicas = make([]ComponentLayerMetrics, 0, len(standbys))
	for _, podName := range standbys {
		var lag *float64
		if out.connectedOnPrimary[podName] {
			if seconds, ok := snap.lagSeconds[podName]; ok {
				lag = replicationLagSecondsPtr(seconds)
			}
		}
		replicaLayer := c.buildDBReplicaLayer(ctx, ns, podName, lag)
		replicaLayer.ConnectedOnPrimary = out.connectedOnPrimary[podName]
		replicaLayer.RequestRate = rpsByLayer[podName]
		out.replicas = append(out.replicas, replicaLayer)
	}
	applyReplicaPromotionPreview(out.replicas, out.connectedOnPrimary, snap.syncState, snap.lagBytes)
	partitionLag := c.replicaPartitionLagBytesByPod(ctx, ns, writerPod, standbys, out.connectedOnPrimary, snap.writerLSN, snap.writerLSNOk)
	for i := range out.replicas {
		if out.replicas[i].ConnectedOnPrimary {
			continue
		}
		if lag, ok := partitionLag[out.replicas[i].ID]; ok {
			out.replicas[i].LagBytes = lagBytesPtr(lag)
		}
	}
	c.enrichDBLayerEngineMetrics(ctx, ns, &out, clusterStorageIO)
	c.enrichDBLayerAppSQL(ctx, ns, &out)
	return out
}

// applyDBReplicaLayers attaches db instance layers and replica StatefulSet rollout counts.
func applyDBReplicaLayers(cm *ComponentMetrics, split dbReplicaLayerSplit) {
	if cm == nil || (split.writerPod == "" && len(split.replicaPods) == 0) {
		return
	}
	layers := make([]ComponentLayerMetrics, 0, 1+len(split.replicas))
	layers = append(layers, split.primary)
	layers = append(layers, split.replicas...)
	cm.Layers = layers
	br := split.counts
	cm.BackendReplicas = &br
	cm.dbWriterPod = split.writerPod
}

// syncDBReplicaLayerRestarts copies restart counts from runtimeHealth pod rows onto matching layers.
func syncDBReplicaLayerRestarts(cm *ComponentMetrics) {
	if cm == nil || len(cm.Layers) == 0 || cm.RuntimeHealth == nil {
		return
	}
	for i := range cm.Layers {
		layer := &cm.Layers[i]
		matchName := layer.ID
		if layer.Role == dbLayerRolePrimary && cm.dbWriterPod != "" {
			matchName = cm.dbWriterPod
		}
		for _, pod := range cm.RuntimeHealth.Pods {
			if pod.Name != matchName {
				continue
			}
			if pod.RestartCount > layer.RestartCount {
				layer.RestartCount = pod.RestartCount
			}
		}
	}
}
