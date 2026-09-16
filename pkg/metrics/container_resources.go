package metrics

import (
	"context"
	"fmt"
	"regexp"
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
	list, err := c.metricsClient.ListPodMetrics(ctx, ns)
	if err != nil {
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
