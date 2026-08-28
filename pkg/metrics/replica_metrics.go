package metrics

import (
	"context"
	"fmt"
	"time"

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

func httpPodLatencyQuantile(ctx context.Context, c *MetricsCollector, nsSel, rangeVec string, quantile float64) map[string]float64 {
	if c == nil {
		return nil
	}
	q := fmt.Sprintf(
		`histogram_quantile(%.2f, sum by (pod, le) (rate(http_request_duration_seconds_bucket{%s}%s)))`,
		quantile, nsSel, rangeVec,
	)
	return c.queryMultiResult(ctx, q, "pod")
}

func workerPodLatencyQuantile(ctx context.Context, c *MetricsCollector, ns, rangeVec string, quantile float64) map[string]float64 {
	if c == nil {
		return nil
	}
	q := fmt.Sprintf(
		`histogram_quantile(%.2f, sum by (pod, le) (rate({__name__=~".*_worker_processing_duration_seconds_bucket",namespace=%q}%s)))`,
		quantile, ns, rangeVec,
	)
	return c.queryMultiResult(ctx, q, "pod")
}

func (c *MetricsCollector) appServerReplicaSeries(ctx context.Context, ns string) (
	rps, p50, p95, p99 map[string]float64,
	p50Session, p95Session, p99Session map[string]float64,
	cpu, errRate, totalReqs, totalErrs5xx map[string]float64,
	memBytes map[string]int64,
) {
	nsSel := fmt.Sprintf(`namespace=%q`, ns)
	podSel := fmt.Sprintf(`namespace=%q,pod=~"app-server.*"`, ns)
	podRe := "app-server.*"
	liveRange := promRateRange()

	rps = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (rate(http_requests_total{%s}%s))`, nsSel, liveRange), "pod")
	p50 = httpPodLatencyQuantile(ctx, c, nsSel, liveRange, 0.50)
	p95 = httpPodLatencyQuantile(ctx, c, nsSel, liveRange, 0.95)
	p99 = httpPodLatencyQuantile(ctx, c, nsSel, liveRange, 0.99)
	cpu = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (rate(process_cpu_seconds_total{namespace=%q,pod=~%q}%s))`, ns, podRe, liveRange), "pod")
	errRate = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (rate(http_requests_total{%s,status=~"5.."}%s))`, nsSel, liveRange), "pod")
	totalReqs = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (http_requests_total{%s})`, podSel), "pod")
	totalErrs5xx = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (http_requests_total{%s,status=~"5.."})`, podSel), "pod")
	memBytes = c.metricsAPIPodContainerMemory(ctx, ns, podRe, "app-server")

	sessionRange := sessionP99PromRange(c.sessionAcc.Elapsed(ns, time.Now()))
	p50Session = httpPodLatencyQuantile(ctx, c, nsSel, sessionRange, 0.50)
	p95Session = httpPodLatencyQuantile(ctx, c, nsSel, sessionRange, 0.95)
	p99Session = httpPodLatencyQuantile(ctx, c, nsSel, sessionRange, 0.99)
	return
}

func (c *MetricsCollector) listAppServerPods(ctx context.Context, ns string) (names []string, ready map[string]bool, counts BackendReplicas) {
	ready = make(map[string]bool)
	if c == nil || c.clientset == nil {
		return nil, ready, counts
	}
	depList, err := c.clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=app-server",
	})
	if err == nil {
		for _, dep := range depList.Items {
			if dep.Spec.Replicas != nil {
				counts.Desired += *dep.Spec.Replicas
			}
		}
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

func (c *MetricsCollector) workerReplicaSeries(ctx context.Context, ns string) (
	rps, p50, p95, p99 map[string]float64,
	p50Session, p95Session, p99Session map[string]float64,
	cpu, errRate, totalProcessed, totalErrors map[string]float64,
	memBytes map[string]int64,
) {
	podRe := "worker.*"
	liveRange := promRateRange()

	rps = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (rate({__name__=~".*_worker_processed_total",namespace=%q}%s))`, ns, liveRange), "pod")
	p50 = workerPodLatencyQuantile(ctx, c, ns, liveRange, 0.50)
	p95 = workerPodLatencyQuantile(ctx, c, ns, liveRange, 0.95)
	p99 = workerPodLatencyQuantile(ctx, c, ns, liveRange, 0.99)
	cpu = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (rate(process_cpu_seconds_total{namespace=%q,pod=~%q}%s))`, ns, podRe, liveRange), "pod")
	errRate = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) (rate({__name__=~".*_worker_errors_total",namespace=%q}%s))`, ns, liveRange), "pod")
	totalProcessed = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) ({__name__=~".*_worker_processed_total",namespace=%q})`, ns), "pod")
	totalErrors = c.queryMultiResult(ctx, fmt.Sprintf(`sum by (pod) ({__name__=~".*_worker_errors_total",namespace=%q})`, ns), "pod")
	memBytes = c.metricsAPIPodContainerMemory(ctx, ns, podRe, "worker")

	sessionRange := sessionP99PromRange(c.sessionAcc.Elapsed(ns, time.Now()))
	p50Session = workerPodLatencyQuantile(ctx, c, ns, sessionRange, 0.50)
	p95Session = workerPodLatencyQuantile(ctx, c, ns, sessionRange, 0.95)
	p99Session = workerPodLatencyQuantile(ctx, c, ns, sessionRange, 0.99)
	return
}

func (c *MetricsCollector) listWorkerPods(ctx context.Context, ns string) (names []string, ready map[string]bool, counts BackendReplicas) {
	ready = make(map[string]bool)
	if c == nil || c.clientset == nil {
		return nil, ready, counts
	}
	depList, err := c.clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=worker",
	})
	if err == nil {
		for _, dep := range depList.Items {
			if dep.Spec.Replicas != nil {
				counts.Desired += *dep.Spec.Replicas
			}
		}
	}
	list, err := c.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=worker,deployment=%s", ns),
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
	rps, p50, p95, p99 map[string]float64,
	p50Session, p95Session, p99Session map[string]float64,
	cpu, errRate, totalReqs, totalErrs5xx map[string]float64,
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
			TotalRequests: totalReqs[name],
			P50Latency:    p50[name],
			P95Latency:    p95[name],
			P99Latency:    p99[name],
			TotalErrors9m: totalErrs5xx[name],
			P50Latency9m:  p50Session[name],
			P95Latency9m:  p95Session[name],
			P99Latency9m:  p99Session[name],
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
	for name := range p50 {
		add(name)
	}
	for name := range p95 {
		add(name)
	}
	for name := range p99 {
		add(name)
	}
	for name := range p50Session {
		add(name)
	}
	for name := range p95Session {
		add(name)
	}
	for name := range p99Session {
		add(name)
	}
	for name := range totalReqs {
		add(name)
	}
	for name := range totalErrs5xx {
		add(name)
	}
	for name := range memBytes {
		add(name)
	}
	return out
}
