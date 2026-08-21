package metrics

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	dbReplicaAppLabel        = "postgres-replica"
	dbReplicaDeploymentName  = "postgres-replica"
	dbLayerRolePrimary       = "primary"
	dbLayerRoleReplica       = "replica"
	dbLayerEnginePostgreSQL  = "postgresql"
	dbLayerIDPostgresPrimary = "postgres"
	dbReplicaPodSelector     = "postgres-replica.*"
	dbPrimaryPodSelector     = "postgres.*"
	// postgresExporterReplayLagMetric is from deployer custom query block replication_replay_lag.
	postgresExporterReplayLagMetric = "pg_replication_replay_lag_replay_lag_seconds"
)

// dbReplicaLayerSplit holds per-instance Postgres layers when read replicas are deployed.
type dbReplicaLayerSplit struct {
	replicaPods []string
	ready       map[string]bool
	counts      BackendReplicas
	primary     ComponentLayerMetrics
	replicas    []ComponentLayerMetrics
}

// listDBReplicaPods returns replica pod names, readiness, and Deployment desired/ready counts.
func (c *MetricsCollector) listDBReplicaPods(ctx context.Context, ns string) (names []string, ready map[string]bool, counts BackendReplicas) {
	ready = make(map[string]bool)
	if c == nil || c.clientset == nil {
		return nil, ready, counts
	}
	depList, err := c.clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s", dbReplicaAppLabel),
	})
	if err == nil {
		for _, dep := range depList.Items {
			if dep.Spec.Replicas != nil {
				counts.Desired += *dep.Spec.Replicas
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
	return names, ready, counts
}

// postgresPrimaryReplayLagByAppQuery reads replay lag seconds from the primary postgres-exporter
// custom query (pg_stat_replication.replay_lag), keyed by application_name (= replica pod hostname).
func postgresPrimaryReplayLagByAppQuery(ns string) string {
	return fmt.Sprintf(
		`max by (application_name) (%s{namespace=%q,pod=~%q,pod!~%q})`,
		postgresExporterReplayLagMetric, ns, dbPrimaryPodSelector, dbReplicaPodSelector,
	)
}

// postgresReplicaLagFallbackByPodQuery is used when the primary custom series is absent.
func postgresReplicaLagFallbackByPodQuery(ns string) string {
	return fmt.Sprintf(
		`max by (pod) (pg_replication_lag{namespace=%q,pod=~%q})`,
		ns, dbReplicaPodSelector,
	)
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

func (c *MetricsCollector) buildDBPrimaryLayer(ctx context.Context, ns string) ComponentLayerMetrics {
	layer := ComponentLayerMetrics{
		ID:     dbLayerIDPostgresPrimary,
		Role:   dbLayerRolePrimary,
		Engine: dbLayerEnginePostgreSQL,
	}
	if c == nil {
		return layer
	}
	cpuRes := c.queryContainerResourceCPU(ctx, ns, dbPrimaryPodSelector, "postgres")
	memRes := c.queryContainerResourceMemory(ctx, ns, dbPrimaryPodSelector, "postgres")
	layer.CPUUsage = cpuRes.Usage
	layer.CPUIsEstimated = cpuRes.Estimated
	layer.CPUSource = cpuRes.Source
	layer.MemoryBytes = int64(memRes.Usage)
	layer.MemIsEstimated = memRes.Estimated
	layer.MemSource = memRes.Source
	layer.CPULimit = c.kubeLimitCPU(ctx, ns, dbPrimaryPodSelector, "postgres")
	layer.MemoryLimit = c.kubeLimitMem(ctx, ns, dbPrimaryPodSelector, "postgres")
	return layer
}

func (c *MetricsCollector) buildDBReplicaLayer(ctx context.Context, ns, podName string, lagSeconds float64) ComponentLayerMetrics {
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
	cpuRes := c.queryContainerResourceCPU(ctx, ns, podRe, "postgres")
	memRes := c.queryContainerResourceMemory(ctx, ns, podRe, "postgres")
	layer.CPUUsage = cpuRes.Usage
	layer.CPUIsEstimated = cpuRes.Estimated
	layer.CPUSource = cpuRes.Source
	layer.MemoryBytes = int64(memRes.Usage)
	layer.MemIsEstimated = memRes.Estimated
	layer.MemSource = memRes.Source
	layer.CPULimit = c.kubeLimitCPU(ctx, ns, podRe, "postgres")
	layer.MemoryLimit = c.kubeLimitMem(ctx, ns, podRe, "postgres")
	return layer
}

// collectDBReplicaLayers builds primary + per-replica instance layers when postgres-replica pods exist.
func (c *MetricsCollector) collectDBReplicaLayers(ctx context.Context, ns string) dbReplicaLayerSplit {
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
	out.replicas = make([]ComponentLayerMetrics, 0, len(names))
	for _, podName := range names {
		out.replicas = append(out.replicas, c.buildDBReplicaLayer(ctx, ns, podName, lagByPod[podName]))
	}
	return out
}

// applyDBReplicaLayers attaches db instance layers and replica Deployment rollout counts.
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
