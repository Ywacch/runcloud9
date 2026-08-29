package diagnostics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/semaphore"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	maxPods              = 20
	maxDeployments       = 20
	maxServices          = 20
	maxEvents            = 100
	maxContainersForLogs = 8
	maxContainerLogLines = int64(50)
	maxAPILogLines       = int64(200)
	maxArtifactSizeBytes = 512 * 1024
	maxPressureScan      = int64(200)
	maxPressureKeep      = 20
	schemaVersion        = 2
)

var pressurePattern = regexp.MustCompile(`(?i)failedscheduling|nottriggerscaleup|triggeredscaleup|quota|nodenotready`)

// Collector gathers bounded diagnostics snapshots and uploads them to GCS.
type Collector struct {
	clientset kubernetes.Interface
	bucket    string
	gcsClient *storage.Client
	sem       *semaphore.Weighted
}

// NewCollector constructs a diagnostics collector with a shared GCS client.
func NewCollector(clientset kubernetes.Interface, bucket string) (*Collector, error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("new gcs client: %w", err)
	}
	return &Collector{
		clientset: clientset,
		bucket:    bucket,
		gcsClient: client,
		sem:       semaphore.NewWeighted(3),
	}, nil
}

// Collect gathers evidence for a namespace and uploads one JSON artifact to GCS.
func (c *Collector) Collect(ctx context.Context, params CollectParams) (string, []FailureSignature, bool, error) {
	if err := c.sem.Acquire(ctx, 1); err != nil {
		return "", nil, true, fmt.Errorf("acquire semaphore: %w", err)
	}
	defer c.sem.Release(1)

	start := time.Now()
	artifact := DiagnosticArtifact{
		SchemaVersion:      schemaVersion,
		CollectedAt:        time.Now().UTC().Format(time.RFC3339),
		CollectionReason:   params.CollectionReason,
		Namespace:          params.Namespace,
		Template:           params.Template,
		Outcome:            params.Outcome,
		OrchestrationError: RedactString(params.OrcError),
		Logs:               make(map[string][]string),
	}
	if artifact.CollectionReason == "" {
		artifact.CollectionReason = "unknown"
	}
	if params.CreatedAt != "" || params.RunningAt != "" || params.DurationSeconds > 0 {
		artifact.Lifecycle = &LifecycleInfo{
			CreatedAt:       params.CreatedAt,
			RunningAt:       params.RunningAt,
			CollectedAt:     artifact.CollectedAt,
			Reason:          artifact.CollectionReason,
			DurationSeconds: params.DurationSeconds,
		}
	}

	pods, podWarnings, podsTruncated := c.collectPods(ctx, params.Namespace, start)
	artifact.Pods = pods
	artifact.PodsTruncated = podsTruncated
	artifact.CollectionWarnings = append(artifact.CollectionWarnings, podWarnings...)

	deployments, deploymentWarnings, deploymentsTruncated := c.collectDeployments(ctx, params.Namespace)
	artifact.Deployments = deployments
	artifact.DeploymentsTruncated = deploymentsTruncated
	artifact.CollectionWarnings = append(artifact.CollectionWarnings, deploymentWarnings...)

	services, serviceWarnings, servicesTruncated := c.collectServices(ctx, params.Namespace)
	artifact.Services = services
	artifact.ServicesTruncated = servicesTruncated
	artifact.CollectionWarnings = append(artifact.CollectionWarnings, serviceWarnings...)

	events, eventsErr := c.collectEvents(ctx, params.Namespace)
	artifact.Events = events
	if eventsErr != nil {
		artifact.EventsCollectionError = eventsErr.Error()
		artifact.CollectionWarnings = append(artifact.CollectionWarnings, "events_collection_error: "+eventsErr.Error())
	}

	userLogs, apiLogs, logWarnings := c.collectLogs(ctx, params.Namespace)
	artifact.Logs = userLogs
	artifact.APILogs = apiLogs
	artifact.CollectionWarnings = append(artifact.CollectionWarnings, logWarnings...)

	artifact.Signatures = ExtractSignatures(artifact.Events, artifact.OrchestrationError, artifact.Logs, artifact.Pods)
	if shouldCollectClusterPressure() {
		artifact.ClusterPressure = c.collectClusterPressure(ctx, time.Now().UTC())
	}
	partial := ctx.Err() != nil
	if partial {
		artifact.CollectionWarnings = append(artifact.CollectionWarnings, "collection deadline exceeded")
	}
	artifact.Partial = partial
	artifact.CollectionDuration = time.Since(start).String()

	payload, err := json.Marshal(artifact)
	if err != nil {
		return "", artifact.Signatures, artifact.Partial, fmt.Errorf("marshal artifact: %w", err)
	}
	if len(payload) > maxArtifactSizeBytes {
		artifact.Logs = map[string][]string{}
		artifact.LogsDropped = true
		artifact.CollectionWarnings = append(artifact.CollectionWarnings, "logs dropped due to artifact size limit")
		payload, err = json.Marshal(artifact)
		if err != nil {
			return "", artifact.Signatures, artifact.Partial, fmt.Errorf("marshal size-trimmed artifact: %w", err)
		}
	}

	objectKey := fmt.Sprintf("%s/%s.json", params.Namespace, time.Now().UTC().Format("2006-01-02T15-04-05Z"))
	url := fmt.Sprintf("gs://%s/%s", c.bucket, objectKey)
	if err := c.uploadJSON(ctx, objectKey, payload); err != nil {
		if retryErr := c.uploadJSON(ctx, objectKey, payload); retryErr != nil {
			return "", artifact.Signatures, artifact.Partial, fmt.Errorf("upload artifact: %w", retryErr)
		}
	}

	return url, artifact.Signatures, artifact.Partial, nil
}

func (c *Collector) uploadJSON(ctx context.Context, objectKey string, body []byte) error {
	writer := c.gcsClient.Bucket(c.bucket).Object(objectKey).NewWriter(ctx)
	writer.ContentType = "application/json"
	if _, err := writer.Write(body); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write gcs object: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close gcs object: %w", err)
	}
	return nil
}

func (c *Collector) collectPods(ctx context.Context, namespace string, start time.Time) ([]PodSnapshot, []string, bool) {
	list, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, []string{"pods_collection_error: " + err.Error()}, false
	}
	out := make([]PodSnapshot, 0, min(len(list.Items), maxPods))
	truncated := false
	for i, pod := range list.Items {
		if i >= maxPods {
			truncated = true
			break
		}
		out = append(out, podSnapshot(pod, start))
	}
	var warnings []string
	if truncated {
		warnings = append(warnings, "pods list truncated to 20")
	}
	return out, warnings, truncated
}

func (c *Collector) collectDeployments(ctx context.Context, namespace string) ([]DeploymentSnapshot, []string, bool) {
	list, err := c.clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, []string{"deployments_collection_error: " + err.Error()}, false
	}
	out := make([]DeploymentSnapshot, 0, min(len(list.Items), maxDeployments))
	truncated := false
	for i, d := range list.Items {
		if i >= maxDeployments {
			truncated = true
			break
		}
		out = append(out, deploymentSnapshot(d))
	}
	var warnings []string
	if truncated {
		warnings = append(warnings, "deployments list truncated to 20")
	}
	return out, warnings, truncated
}

func (c *Collector) collectServices(ctx context.Context, namespace string) ([]ServiceSnapshot, []string, bool) {
	list, err := c.clientset.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, []string{"services_collection_error: " + err.Error()}, false
	}
	out := make([]ServiceSnapshot, 0, min(len(list.Items), maxServices))
	truncated := false
	for i, svc := range list.Items {
		if i >= maxServices {
			truncated = true
			break
		}
		ports := make([]int32, 0, len(svc.Spec.Ports))
		for _, p := range svc.Spec.Ports {
			ports = append(ports, p.Port)
		}
		out = append(out, ServiceSnapshot{Name: svc.Name, Type: string(svc.Spec.Type), Ports: ports})
	}
	var warnings []string
	if truncated {
		warnings = append(warnings, "services list truncated to 20")
	}
	return out, warnings, truncated
}

func (c *Collector) collectEvents(ctx context.Context, namespace string) ([]EventSnapshot, error) {
	list, err := c.clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{Limit: maxEvents})
	if err != nil {
		return []EventSnapshot{}, err
	}
	out := make([]EventSnapshot, 0, len(list.Items))
	for _, evt := range list.Items {
		timestamp := evt.EventTime.Time
		if timestamp.IsZero() {
			timestamp = evt.LastTimestamp.Time
		}
		if timestamp.IsZero() {
			timestamp = evt.FirstTimestamp.Time
		}
		out = append(out, EventSnapshot{
			Reason:    evt.Reason,
			Message:   RedactString(evt.Message),
			Type:      evt.Type,
			Object:    strings.ToLower(evt.InvolvedObject.Kind) + "/" + evt.InvolvedObject.Name,
			Count:     evt.Count,
			Timestamp: timestamp.UTC().Format(time.RFC3339),
		})
	}
	return out, nil
}

func (c *Collector) collectLogs(ctx context.Context, namespace string) (map[string][]string, []string, []string) {
	logs := make(map[string][]string)
	var apiLogs []string
	var warnings []string

	podList, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return logs, apiLogs, append(warnings, "pod_log_list_error: "+err.Error())
	}

	collected := 0
	for _, pod := range podList.Items {
		for _, container := range podLogContainers(pod) {
			if collected >= maxContainersForLogs {
				warnings = append(warnings, "container logs truncated to first 8 containers")
				break
			}
			lines, logErr := c.readPodLogs(ctx, namespace, pod.Name, container.Name, maxContainerLogLines)
			if logErr != nil {
				warnings = append(warnings, fmt.Sprintf("log_collection_error:%s/%s: %v", pod.Name, container.Name, logErr))
				continue
			}
			key := pod.Name + "/" + container.Name
			logs[key] = lines
			collected++
		}
		if collected >= maxContainersForLogs {
			break
		}
	}

	apiPods, err := c.clientset.CoreV1().Pods("cloud9-system").List(ctx, metav1.ListOptions{LabelSelector: "app=cloud9-api"})
	if err != nil {
		return logs, apiLogs, append(warnings, "api_log_pod_list_error: "+err.Error())
	}
	if len(apiPods.Items) == 0 {
		return logs, apiLogs, append(warnings, "api_log_collection_warning: no cloud9-api pods found")
	}
	seen := make(map[string]struct{})
	for _, p := range apiPods.Items {
		lines, logErr := c.readPodLogs(ctx, "cloud9-system", p.Name, "cloud9-api", maxAPILogLines)
		if logErr != nil {
			warnings = append(warnings, fmt.Sprintf("api_log_collection_error:%s: %v", p.Name, logErr))
			continue
		}
		for _, line := range lines {
			if strings.Contains(line, namespace) {
				if _, ok := seen[line]; ok {
					continue
				}
				seen[line] = struct{}{}
				apiLogs = append(apiLogs, line)
			}
		}
	}
	sort.Strings(apiLogs)
	return logs, apiLogs, warnings
}

func podLogContainers(pod corev1.Pod) []corev1.Container {
	out := make([]corev1.Container, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	out = append(out, pod.Spec.InitContainers...)
	out = append(out, pod.Spec.Containers...)
	return out
}

func (c *Collector) readPodLogs(ctx context.Context, namespace, podName, containerName string, tailLines int64) ([]string, error) {
	req := c.clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: containerName,
		TailLines: &tailLines,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("stream logs: %w", err)
	}
	defer stream.Close()
	body, err := io.ReadAll(stream)
	if err != nil {
		return nil, fmt.Errorf("read logs: %w", err)
	}
	rawLines := strings.Split(string(body), "\n")
	out := make([]string, 0, len(rawLines))
	for _, line := range rawLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		out = append(out, RedactString(line))
	}
	return out, nil
}

func shouldCollectClusterPressure() bool {
	return strings.EqualFold(os.Getenv("DDC_CLUSTER_PRESSURE"), "true")
}

func (c *Collector) collectClusterPressure(ctx context.Context, now time.Time) *ClusterPressure {
	out := &ClusterPressure{
		Events:      []EventSnapshot{},
		CollectedAt: now.Format(time.RFC3339),
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < 3*time.Second {
		out.Warning = "skipped: insufficient time remaining"
		return out
	}
	list, err := c.clientset.CoreV1().Events("kube-system").List(ctx, metav1.ListOptions{Limit: maxPressureScan})
	if err != nil {
		out.Warning = fmt.Sprintf("cluster pressure collection failed: %v", err)
		return out
	}
	filtered, truncated := filterClusterPressureEvents(list.Items, now)
	out.Events = filtered
	out.EventsTruncated = truncated
	return out
}

func filterClusterPressureEvents(events []corev1.Event, now time.Time) ([]EventSnapshot, bool) {
	keep := make([]EventSnapshot, 0, maxPressureKeep)
	cutoff := now.Add(-5 * time.Minute)
	truncated := false
	for _, evt := range events {
		ts := evt.EventTime.Time
		if ts.IsZero() {
			ts = evt.LastTimestamp.Time
		}
		if ts.IsZero() {
			ts = evt.FirstTimestamp.Time
		}
		if ts.IsZero() || ts.Before(cutoff) {
			continue
		}
		text := evt.Reason + " " + evt.Message
		if !pressurePattern.MatchString(text) {
			continue
		}
		if len(keep) >= maxPressureKeep {
			truncated = true
			break
		}
		keep = append(keep, EventSnapshot{
			Reason:    evt.Reason,
			Message:   RedactString(evt.Message),
			Type:      evt.Type,
			Object:    strings.ToLower(evt.InvolvedObject.Kind) + "/" + evt.InvolvedObject.Name,
			Count:     evt.Count,
			Timestamp: ts.UTC().Format(time.RFC3339),
		})
	}
	return keep, truncated
}

func podSnapshot(pod corev1.Pod, start time.Time) PodSnapshot {
	conditions := make([]string, 0, len(pod.Status.Conditions))
	ready := false
	for _, cond := range pod.Status.Conditions {
		conditions = append(conditions, fmt.Sprintf("%s=%s", cond.Type, cond.Status))
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	containers := make([]ContainerStatus, 0, len(pod.Status.ContainerStatuses))
	for _, c := range pod.Status.ContainerStatuses {
		state := "unknown"
		switch {
		case c.State.Running != nil:
			state = "running"
		case c.State.Waiting != nil:
			state = "waiting:" + c.State.Waiting.Reason
		case c.State.Terminated != nil:
			state = "terminated:" + c.State.Terminated.Reason
		}
		containers = append(containers, ContainerStatus{
			Name:         c.Name,
			Image:        c.Image,
			Ready:        c.Ready,
			RestartCount: c.RestartCount,
			State:        state,
		})
	}
	age := time.Since(pod.CreationTimestamp.Time).Round(time.Second).String()
	if pod.CreationTimestamp.Time.IsZero() {
		age = time.Since(start).Round(time.Second).String()
	}
	return PodSnapshot{
		Name:       pod.Name,
		Phase:      string(pod.Status.Phase),
		Ready:      ready,
		Conditions: conditions,
		Node:       pod.Spec.NodeName,
		Containers: containers,
		Age:        age,
	}
}

func deploymentSnapshot(d appsv1.Deployment) DeploymentSnapshot {
	ready := d.Status.AvailableReplicas >= d.Status.Replicas && d.Status.Replicas > 0
	return DeploymentSnapshot{
		Name:              d.Name,
		Replicas:          d.Status.Replicas,
		AvailableReplicas: d.Status.AvailableReplicas,
		Ready:             ready,
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
