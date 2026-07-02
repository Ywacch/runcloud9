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

// queryContainerResourceCPU returns live CPU for a named container: cAdvisor → Metrics API → kube requests.
func (c *MetricsCollector) queryContainerResourceCPU(ctx context.Context, ns, podRe, container string) containerResource {
	primary := fmt.Sprintf(
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~%q,container="%s"}[30s]))`,
		ns, podRe, container,
	)
	val, present, err := c.queryScalar(ctx, primary)
	if err == nil && present {
		if c.debugMetrics {
			c.debugCompare(container, "cpu", val, 0, ResourceSourceCadvisor, "")
		}
		return containerResource{Usage: val, Estimated: false, Source: ResourceSourceCadvisor}
	}

	if cpu, ok := c.queryMetricsAPIContainerCPU(ctx, ns, podRe, container); ok {
		if c.debugMetrics {
			c.debugCompare(container, "cpu", cpu, 0, ResourceSourceMetricsAPI, "")
		}
		return containerResource{Usage: cpu, Estimated: false, Source: ResourceSourceMetricsAPI}
	}

	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,container="%s",resource="cpu"})`,
		ns, podRe, container,
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
	primary := fmt.Sprintf(
		`sum(container_memory_working_set_bytes{namespace=%q,pod=~%q,container="%s"})`,
		ns, podRe, container,
	)
	val, present, err := c.queryScalar(ctx, primary)
	if err == nil && present {
		if c.debugMetrics {
			c.debugCompare(container, "mem", val, 0, ResourceSourceCadvisor, "")
		}
		return containerResource{Usage: val, Estimated: false, Source: ResourceSourceCadvisor}
	}

	if mem, ok := c.queryMetricsAPIContainerMemory(ctx, ns, podRe, container); ok {
		if c.debugMetrics {
			c.debugCompare(container, "mem", float64(mem), 0, ResourceSourceMetricsAPI, "")
		}
		return containerResource{Usage: float64(mem), Estimated: false, Source: ResourceSourceMetricsAPI}
	}

	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,container="%s",resource="memory"})`,
		ns, podRe, container,
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

func compilePodRE(podRe string) (*regexp.Regexp, error) {
	return regexp.Compile("^" + podRe + "$")
}

func (c *MetricsCollector) sumMetricsAPIContainer(ctx context.Context, ns, podRe, container string) (cpuCores float64, memBytes int64, ok bool) {
	if c == nil || c.metricsClient == nil {
		return 0, 0, false
	}
	re, err := compilePodRE(podRe)
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
		if !re.MatchString(pm.Name) {
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

func (c *MetricsCollector) queryMetricsAPIContainerCPU(ctx context.Context, ns, podRe, container string) (float64, bool) {
	cpu, _, ok := c.sumMetricsAPIContainer(ctx, ns, podRe, container)
	return cpu, ok
}

func (c *MetricsCollector) queryMetricsAPIContainerMemory(ctx context.Context, ns, podRe, container string) (int64, bool) {
	_, mem, ok := c.sumMetricsAPIContainer(ctx, ns, podRe, container)
	return mem, ok
}
