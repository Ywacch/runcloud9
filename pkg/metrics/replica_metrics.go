package metrics

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func podReadyStatus(pod corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (c *MetricsCollector) appServerReplicaSeries(ctx context.Context, ns string) (
	rps, p99, cpu, errRate, totalReqs9m map[string]float64,
	memBytes map[string]int64,
) {
	nsSel := fmt.Sprintf(`namespace=%q`, ns)
	podSel := fmt.Sprintf(`namespace=%q,pod=~"app-server.*"`, ns)
	podRe := "app-server.*"
	return c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (rate(http_requests_total{%s}%s))`, nsSel, promRateRange()), "pod"),
		c.queryMultiResult(ctx, fmt.Sprintf(`histogram_quantile(0.99, sum by (pod, le) (rate(http_request_duration_seconds_bucket{%s}%s)))`, nsSel, promRateRange()), "pod"),
		c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (rate(process_cpu_seconds_total{namespace=%q,pod=~%q}%s))`, ns, podRe, promRateRange()), "pod"),
		c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (rate(http_requests_total{%s,status=~"5.."}%s))`, nsSel, promRateRange()), "pod"),
		c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (increase(http_requests_total{%s}[9m]))`, podSel), "pod"),
		c.metricsAPIPodContainerMemory(ctx, ns, podRe, "app-server")
}

func (c *MetricsCollector) listAppServerPods(ctx context.Context, ns string) (names []string, ready map[string]bool, counts BackendReplicas) {
	ready = make(map[string]bool)
	if c == nil || c.clientset == nil {
		return nil, ready, counts
	}
	dep, err := c.clientset.AppsV1().Deployments(ns).Get(ctx, "app-server", metav1.GetOptions{})
	if err == nil && dep.Spec.Replicas != nil {
		counts.Desired = *dep.Spec.Replicas
	}
	list, err := c.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=app-server,deployment=%s", ns),
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

func mergePodMetricMaps(
	names []string,
	ready map[string]bool,
	rps, p99, cpu, errRate, totalReqs9m map[string]float64,
	memBytes map[string]int64,
) []PodMetrics {
	seen := make(map[string]struct{})
	var out []PodMetrics
	add := func(name string) {
		if name == "" {
			return
		}
		if _, dup := seen[name]; dup {
			return
		}
		seen[name] = struct{}{}
		out = append(out, PodMetrics{
			Name:          name,
			Ready:         ready[name],
			RequestRate:   rps[name],
			TotalRequests: totalReqs9m[name],
			P99Latency:    p99[name],
			CPUUsage:      cpu[name],
			MemoryBytes:   memBytes[name],
			ErrorRate:     errRate[name],
		})
	}
	for _, n := range names {
		add(n)
	}
	for name := range rps {
		add(name)
	}
	for name := range p99 {
		add(name)
	}
	for name := range totalReqs9m {
		add(name)
	}
	for name := range memBytes {
		add(name)
	}
	return out
}
