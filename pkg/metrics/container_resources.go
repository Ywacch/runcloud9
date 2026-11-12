package metrics

import (
	"context"
	"fmt"
	"regexp"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
	corev1 "k8s.io/api/core/v1"
)

// Resource usage source identifiers for ComponentMetrics cpuSource/memSource.
const (
	ResourceSourceCadvisor   = "cadvisor"
	ResourceSourceMetricsAPI = "metrics_api"
	ResourceSourceRequests   = "requests"
)

type containerResource struct {
	Usage     float64
	Estimated bool
	Source    string
}

func promPodLabelSelectors(podRe, excludePodRe string) string {
	if excludePodRe == "" {
		return fmt.Sprintf(`pod=~%q`, podRe)
	}
	return fmt.Sprintf(`pod=~%q,pod!~%q`, podRe, excludePodRe)
}

func kubeLimitCPUPromQL(ns, podRe, excludePodRe, container string) string {
	podSel := promPodLabelSelectors(podRe, excludePodRe)
	return fmt.Sprintf(
		`sum(kube_pod_container_resource_limits{namespace=%q,%s,container="%s",resource="cpu"})`,
		ns, podSel, container,
	)
}

func kubeLimitMemPromQL(ns, podRe, excludePodRe, container string) string {
	podSel := promPodLabelSelectors(podRe, excludePodRe)
	return fmt.Sprintf(
		`sum(kube_pod_container_resource_limits{namespace=%q,%s,container="%s",resource="memory"})`,
		ns, podSel, container,
	)
}

func (c *MetricsCollector) limitsCacheActive() bool {
	return c != nil && (c.inv != nil || c.clusterState != nil)
}

func (c *MetricsCollector) listPodsForLimits(ctx context.Context, ns string) ([]*corev1.Pod, error) {
	if c == nil || ns == "" {
		return nil, nil
	}
	if c.inv != nil {
		return c.inv.Pods(ns)
	}
	if c.clusterState != nil {
		return c.clusterState.Pods(ns)
	}
	return nil, nil
}

func sumPodContainerCPULimits(pods []*corev1.Pod, podRe, excludePodRe, container string) (float64, bool) {
	include, exclude, err := compilePodREWithExclude(podRe, excludePodRe)
	if err != nil {
		return 0, false
	}
	var sum float64
	var found bool
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if !podMatchesSelectors(pod.Name, include, exclude) {
			continue
		}
		for i := range pod.Spec.Containers {
			cont := &pod.Spec.Containers[i]
			if cont.Name != container {
				continue
			}
			q, ok := cont.Resources.Limits[corev1.ResourceCPU]
			if !ok || q.IsZero() {
				continue
			}
			found = true
			sum += float64(q.MilliValue()) / 1000.0
		}
	}
	return sum, found
}

func sumPodContainerMemoryLimits(pods []*corev1.Pod, podRe, excludePodRe, container string) (int64, bool) {
	include, exclude, err := compilePodREWithExclude(podRe, excludePodRe)
	if err != nil {
		return 0, false
	}
	var sum int64
	var found bool
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if !podMatchesSelectors(pod.Name, include, exclude) {
			continue
		}
		for i := range pod.Spec.Containers {
			cont := &pod.Spec.Containers[i]
			if cont.Name != container {
				continue
			}
			q, ok := cont.Resources.Limits[corev1.ResourceMemory]
			if !ok || q.IsZero() {
				continue
			}
			found = true
			sum += q.Value()
		}
	}
	return sum, found
}

func (c *MetricsCollector) kubeLimitCPUExcluding(ctx context.Context, ns, podRe, excludePodRe, container string) float64 {
	promql := kubeLimitCPUPromQL(ns, podRe, excludePodRe, container)
	if c != nil && c.limitsCacheActive() {
		pods, err := c.listPodsForLimits(ctx, ns)
		if err == nil {
			val, present := sumPodContainerCPULimits(pods, podRe, excludePodRe, container)
			c.compareResourceLimit(ctx, signals.ContainerCPULimit, val, present, promql)
			if present {
				return val
			}
			return 0
		}
	}
	v, _ := c.query(ctx, promql)
	return v
}

func (c *MetricsCollector) kubeLimitMemExcluding(ctx context.Context, ns, podRe, excludePodRe, container string) int64 {
	promql := kubeLimitMemPromQL(ns, podRe, excludePodRe, container)
	if c != nil && c.limitsCacheActive() {
		pods, err := c.listPodsForLimits(ctx, ns)
		if err == nil {
			val, present := sumPodContainerMemoryLimits(pods, podRe, excludePodRe, container)
			c.compareResourceLimit(ctx, signals.ContainerMemoryLimit, float64(val), present, promql)
			if present {
				return val
			}
			return 0
		}
	}
	v, _ := c.query(ctx, promql)
	return int64(v)
}

func (c *MetricsCollector) compareResourceLimit(ctx context.Context, id signals.ID, cacheVal float64, cacheOK bool, promql string) {
	if c == nil || !signalCompareEnabled() || !c.limitsCacheActive() {
		return
	}
	oldVal, oldOK, err := c.queryScalarDirect(ctx, promql)
	if err != nil {
		oldOK = false
	}
	match := promValuesMatch(cacheVal, oldVal, cacheOK, oldOK)
	result := "match"
	if !match {
		result = "mismatch"
	}
	if c.signalCompareObserver != nil {
		c.signalCompareObserver(string(id), result)
	}
}

// queryContainerResourceCPU returns live CPU for a named container: cAdvisor → Metrics API → kube requests.
func (c *MetricsCollector) queryContainerResourceCPU(ctx context.Context, ns, podRe, container string) containerResource {
	return c.queryContainerResourceCPUExcluding(ctx, ns, podRe, "", container)
}

// queryContainerResourceCPUExcluding is like queryContainerResourceCPU but omits pods matching excludePodRe.
func (c *MetricsCollector) queryContainerResourceCPUExcluding(ctx context.Context, ns, podRe, excludePodRe, container string) containerResource {
	podSel := promPodLabelSelectors(podRe, excludePodRe)
	primary := fmt.Sprintf(
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,%s,container="%s"}%s))`,
		ns, podSel, container, promRateRange(),
	)
	val, present, err := c.queryScalar(ctx, primary)
	if err == nil && present {
		if c.debugMetrics {
			c.debugCompare(container, "cpu", val, 0, ResourceSourceCadvisor, "")
		}
		return containerResource{Usage: val, Estimated: false, Source: ResourceSourceCadvisor}
	}

	if cpu, ok := c.queryMetricsAPIContainerCPUExcluding(ctx, ns, podRe, excludePodRe, container); ok {
		if c.debugMetrics {
			c.debugCompare(container, "cpu", cpu, 0, ResourceSourceMetricsAPI, "")
		}
		return containerResource{Usage: cpu, Estimated: false, Source: ResourceSourceMetricsAPI}
	}

	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,%s,container="%s",resource="cpu"})`,
		ns, podSel, container,
	)
	fb, fbPresent, _ := c.queryScalar(ctx, fallback)
	if fbPresent && fb > 0 {
		if c.debugMetrics {
			c.debugCompare(container, "cpu", 0, fb, "", ResourceSourceRequests)
		}
		return containerResource{Usage: fb, Estimated: true, Source: ResourceSourceRequests}
	}
	return containerResource{Estimated: true, Source: ResourceSourceRequests}
}

// queryContainerResourceMemory returns live memory bytes for a named container: cAdvisor → Metrics API → kube requests.
func (c *MetricsCollector) queryContainerResourceMemory(ctx context.Context, ns, podRe, container string) containerResource {
	return c.queryContainerResourceMemoryExcluding(ctx, ns, podRe, "", container)
}

// queryContainerResourceMemoryExcluding is like queryContainerResourceMemory but omits pods matching excludePodRe.
func (c *MetricsCollector) queryContainerResourceMemoryExcluding(ctx context.Context, ns, podRe, excludePodRe, container string) containerResource {
	podSel := promPodLabelSelectors(podRe, excludePodRe)
	primary := fmt.Sprintf(
		`sum(container_memory_working_set_bytes{namespace=%q,%s,container="%s"})`,
		ns, podSel, container,
	)
	val, present, err := c.queryScalar(ctx, primary)
	if err == nil && present {
		if c.debugMetrics {
			c.debugCompare(container, "mem", val, 0, ResourceSourceCadvisor, "")
		}
		return containerResource{Usage: val, Estimated: false, Source: ResourceSourceCadvisor}
	}

	if mem, ok := c.queryMetricsAPIContainerMemoryExcluding(ctx, ns, podRe, excludePodRe, container); ok {
		if c.debugMetrics {
			c.debugCompare(container, "mem", float64(mem), 0, ResourceSourceMetricsAPI, "")
		}
		return containerResource{Usage: float64(mem), Estimated: false, Source: ResourceSourceMetricsAPI}
	}

	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,%s,container="%s",resource="memory"})`,
		ns, podSel, container,
	)
	fb, fbPresent, _ := c.queryScalar(ctx, fallback)
	if fbPresent && fb > 0 {
		if c.debugMetrics {
			c.debugCompare(container, "mem", 0, fb, "", ResourceSourceRequests)
		}
		return containerResource{Usage: fb, Estimated: true, Source: ResourceSourceRequests}
	}
	return containerResource{Estimated: true, Source: ResourceSourceRequests}
}

// mergeContainerResources sums usage across containers. Live (non-estimated)
// sources win Estimated/Source so a missing Deployment postgres container
// does not hide a live postgres-replica writer after promote.
func mergeContainerResources(parts ...containerResource) containerResource {
	out := containerResource{Estimated: true, Source: ResourceSourceRequests}
	liveSource := ""
	for _, p := range parts {
		out.Usage += p.Usage
		if p.Estimated {
			continue
		}
		out.Estimated = false
		switch {
		case p.Source == ResourceSourceCadvisor:
			liveSource = ResourceSourceCadvisor
		case liveSource != ResourceSourceCadvisor && p.Source != "":
			liveSource = p.Source
		}
	}
	if !out.Estimated && liveSource != "" {
		out.Source = liveSource
	}
	return out
}

func (c *MetricsCollector) queryPostgresClusterCPU(ctx context.Context, ns string) containerResource {
	if c == nil {
		return containerResource{Estimated: true, Source: ResourceSourceRequests}
	}
	return mergeContainerResources(
		c.queryContainerResourceCPU(ctx, ns, dbPrimaryPodSelector, dbPrimaryContainerName),
		c.queryContainerResourceCPU(ctx, ns, dbReplicaPodSelector, dbReplicaContainerName),
	)
}

func (c *MetricsCollector) queryPostgresClusterMemory(ctx context.Context, ns string) containerResource {
	if c == nil {
		return containerResource{Estimated: true, Source: ResourceSourceRequests}
	}
	return mergeContainerResources(
		c.queryContainerResourceMemory(ctx, ns, dbPrimaryPodSelector, dbPrimaryContainerName),
		c.queryContainerResourceMemory(ctx, ns, dbReplicaPodSelector, dbReplicaContainerName),
	)
}

func (c *MetricsCollector) kubeLimitPostgresClusterCPU(ctx context.Context, ns string) float64 {
	if c == nil {
		return 0
	}
	return c.kubeLimitCPU(ctx, ns, dbPrimaryPodSelector, dbPrimaryContainerName) +
		c.kubeLimitCPU(ctx, ns, dbReplicaPodSelector, dbReplicaContainerName)
}

func (c *MetricsCollector) kubeLimitPostgresClusterMem(ctx context.Context, ns string) int64 {
	if c == nil {
		return 0
	}
	return c.kubeLimitMem(ctx, ns, dbPrimaryPodSelector, dbPrimaryContainerName) +
		c.kubeLimitMem(ctx, ns, dbReplicaPodSelector, dbReplicaContainerName)
}

func compilePodRE(podRe string) (*regexp.Regexp, error) {
	return regexp.Compile("^" + podRe + "$")
}

func compilePodREWithExclude(podRe, excludePodRe string) (include *regexp.Regexp, exclude *regexp.Regexp, err error) {
	include, err = compilePodRE(podRe)
	if err != nil {
		return nil, nil, err
	}
	if excludePodRe == "" {
		return include, nil, nil
	}
	exclude, err = compilePodRE(excludePodRe)
	if err != nil {
		return nil, nil, err
	}
	return include, exclude, nil
}

func podMatchesSelectors(name string, include, exclude *regexp.Regexp) bool {
	if include != nil && !include.MatchString(name) {
		return false
	}
	if exclude != nil && exclude.MatchString(name) {
		return false
	}
	return true
}

func (c *MetricsCollector) sumMetricsAPIContainerExcluding(ctx context.Context, ns, podRe, excludePodRe, container string) (cpuCores float64, memBytes int64, ok bool) {
	if c == nil || c.metricsClient == nil {
		return 0, 0, false
	}
	include, exclude, err := compilePodREWithExclude(podRe, excludePodRe)
	if err != nil {
		return 0, 0, false
	}
	if h := c.MetricsHub(); h != nil {
		if snap := h.Snapshot(); snap != nil {
			var excludeFn func(string) bool
			if exclude != nil {
				excludeFn = exclude.MatchString
			}
			cpuMilli, mem, found, sampled := snap.ContainerUsage(ns, container, include.MatchString, excludeFn)
			if sampled {
				if !found {
					return 0, 0, false
				}
				return float64(cpuMilli) / 1000.0, mem, true
			}
		}
	}
	list, errList := c.metricsClient.ListPodMetrics(ctx, ns)
	if errList != nil {
		return 0, 0, false
	}
	var cpuMilli int64
	found := false
	for _, pm := range list.Items {
		if !podMatchesSelectors(pm.Name, include, exclude) {
			continue
		}
		for _, cont := range pm.Containers {
			if cont.Name != container {
				continue
			}
			found = true
			cpuMilli += cont.Usage.Cpu().MilliValue()
			memBytes += cont.Usage.Memory().Value()
		}
	}
	if !found {
		return 0, 0, false
	}
	return float64(cpuMilli) / 1000.0, memBytes, true
}

func (c *MetricsCollector) sumMetricsAPIContainer(ctx context.Context, ns, podRe, container string) (cpuCores float64, memBytes int64, ok bool) {
	return c.sumMetricsAPIContainerExcluding(ctx, ns, podRe, "", container)
}

func (c *MetricsCollector) queryMetricsAPIContainerCPU(ctx context.Context, ns, podRe, container string) (float64, bool) {
	cpu, _, ok := c.sumMetricsAPIContainer(ctx, ns, podRe, container)
	return cpu, ok
}

func (c *MetricsCollector) queryMetricsAPIContainerCPUExcluding(ctx context.Context, ns, podRe, excludePodRe, container string) (float64, bool) {
	cpu, _, ok := c.sumMetricsAPIContainerExcluding(ctx, ns, podRe, excludePodRe, container)
	return cpu, ok
}

func (c *MetricsCollector) queryMetricsAPIContainerMemory(ctx context.Context, ns, podRe, container string) (int64, bool) {
	_, mem, ok := c.sumMetricsAPIContainer(ctx, ns, podRe, container)
	return mem, ok
}

func (c *MetricsCollector) queryMetricsAPIContainerMemoryExcluding(ctx context.Context, ns, podRe, excludePodRe, container string) (int64, bool) {
	_, mem, ok := c.sumMetricsAPIContainerExcluding(ctx, ns, podRe, excludePodRe, container)
	return mem, ok
}

// metricsAPIPodContainerMemory returns per-pod memory bytes for a container via metrics.k8s.io.
func (c *MetricsCollector) metricsAPIPodContainerMemory(ctx context.Context, ns, podRe, container string) map[string]int64 {
	if c == nil || c.metricsClient == nil {
		return nil
	}
	re, err := compilePodRE(podRe)
	if err != nil {
		return nil
	}
	list, err := c.metricsClient.ListPodMetrics(ctx, ns)
	if err != nil {
		return nil
	}
	out := make(map[string]int64)
	for _, pm := range list.Items {
		if !re.MatchString(pm.Name) {
			continue
		}
		for _, cont := range pm.Containers {
			if cont.Name != container {
				continue
			}
			out[pm.Name] += cont.Usage.Memory().Value()
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
