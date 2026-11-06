package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
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
	ctx = withPromCaller(ctx, "replica")
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

// isLiveInventoryPod reports whether a pod should appear in replica inventory.
// Succeeded/Failed pods (kubectl Completed/Error) and pods with a deletion
// timestamp stay listed by the API after a replacement is created; they are
// not current replicas.
func isLiveInventoryPod(pod corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	default:
		return true
	}
}

// deploymentNameMatchesApp reports whether a Deployment is one of the
// app-server or worker rollouts (including mixed-size names like app-server-small).
func deploymentNameMatchesApp(name, app string) bool {
	return name == app || strings.HasPrefix(name, app+"-")
}

func sumNamedDeploymentReplicas(deps []appsv1.Deployment, app string) int32 {
	var n int32
	for _, dep := range deps {
		if !deploymentNameMatchesApp(dep.Name, app) {
			continue
		}
		if dep.Spec.Replicas != nil {
			n += *dep.Spec.Replicas
		}
	}
	return n
}

func (c *MetricsCollector) listLabeledLivePods(ctx context.Context, ns, app string) (names []string, ready map[string]bool, counts BackendReplicas) {
	ready = make(map[string]bool)
	if c == nil {
		return nil, ready, counts
	}
	if c.inv == nil && c.clientset == nil {
		return nil, ready, counts
	}
	if c.inv != nil {
		if deps, err := c.inv.Deployments(ns); err == nil {
			counts.Desired = sumNamedDeploymentReplicas(deploymentItems(deps), app)
		}
		ptrs, err := c.inv.Pods(ns)
		if err != nil {
			return nil, ready, counts
		}
		items := copyPodsWithAppAndDeployment(ptrs, app, ns)
		names = make([]string, 0, len(items))
		for _, p := range items {
			if !isLiveInventoryPod(p) {
				continue
			}
			isReady := podReadyStatus(p)
			ready[p.Name] = isReady
			names = append(names, p.Name)
			if isReady {
				counts.Ready++
			}
		}
		return names, ready, counts
	}
	// Deployer puts app= labels on the pod template only, not Deployment
	// metadata. Sum spec.replicas by Deployment name so desired stays correct.
	depList, err := c.clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
	if err == nil {
		counts.Desired = sumNamedDeploymentReplicas(depList.Items, app)
	}
	list, err := c.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s,deployment=%s", app, ns),
	})
	if err != nil {
		return nil, ready, counts
	}
	names = make([]string, 0, len(list.Items))
	for _, p := range list.Items {
		if !isLiveInventoryPod(p) {
			continue
		}
		isReady := podReadyStatus(p)
		ready[p.Name] = isReady
		names = append(names, p.Name)
		if isReady {
			counts.Ready++
		}
	}
	return names, ready, counts
}

func (c *MetricsCollector) listAppServerPods(ctx context.Context, ns string) (names []string, ready map[string]bool, counts BackendReplicas) {
	return c.listLabeledLivePods(ctx, ns, "app-server")
}

func (c *MetricsCollector) workerReplicaSeries(ctx context.Context, ns string) (
	rps, p50, p95, p99 map[string]float64,
	p50Session, p95Session, p99Session map[string]float64,
	cpu, errRate, totalProcessed, totalErrors map[string]float64,
	memBytes map[string]int64,
) {
	ctx = withPromCaller(ctx, "replica")
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
	return c.listLabeledLivePods(ctx, ns, "worker")
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
	// Non-nil names means Kubernetes inventory was listed. Leftover Prom
	// series for Succeeded/Failed pods must not reappear as replica rows.
	// Nil names (list failed) still falls back to Prom labels.
	if names != nil {
		return out
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
