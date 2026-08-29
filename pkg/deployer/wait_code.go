package deployer

import (
	"context"
	"errors"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	WaitCodeReady        = "ready"
	WaitCodePending      = "pending"
	WaitCodeStarting     = "starting"
	WaitCodePullingImage = "pulling_image"
	WaitCodeCrashLoop    = "crash_loop"
	WaitCodeConfigError  = "config_error"
	WaitCodeTimeout      = "timeout"
	WaitCodeUnknown      = "unknown"
)

// PodWaitProgress is one row in metricsSummary.replica_status for a canvas node instance.
type PodWaitProgress struct {
	Ordinal  int
	Ready    bool
	WaitCode string
}

// WaitCodeFromKubeReason maps a kubelet waiting reason to a stable provision wait code.
func WaitCodeFromKubeReason(reason string) string {
	switch reason {
	case "CreateContainerConfigError", "InvalidImageName":
		return WaitCodeConfigError
	case "CrashLoopBackOff":
		return WaitCodeCrashLoop
	case "ImagePullBackOff", "ErrImagePull", "PullImage":
		return WaitCodePullingImage
	case "ContainerCreating", "PodInitializing":
		return WaitCodeStarting
	default:
		return WaitCodeStarting
	}
}

func waitCodeFromPod(pod *corev1.Pod) string {
	if podConditionReady(pod) {
		return WaitCodeReady
	}
	if pod.DeletionTimestamp != nil {
		return WaitCodePending
	}
	code := WaitCodeStarting
	scan := func(css []corev1.ContainerStatus) {
		for _, cs := range css {
			if w := cs.State.Waiting; w != nil && w.Reason != "" {
				mapped := WaitCodeFromKubeReason(w.Reason)
				code = maxWaitCodeSeverity(code, mapped)
			}
		}
	}
	scan(pod.Status.InitContainerStatuses)
	scan(pod.Status.ContainerStatuses)
	if pod.Status.Phase == corev1.PodPending && len(pod.Status.ContainerStatuses) == 0 {
		return WaitCodePending
	}
	return code
}

func maxWaitCodeSeverity(current, candidate string) string {
	rank := func(code string) int {
		switch code {
		case WaitCodeConfigError:
			return 5
		case WaitCodeCrashLoop:
			return 4
		case WaitCodePullingImage:
			return 3
		case WaitCodeStarting:
			return 2
		case WaitCodePending:
			return 1
		default:
			return 0
		}
	}
	if rank(candidate) > rank(current) {
		return candidate
	}
	return current
}

// ProvisionDetailFromDeployError maps a deploy failure to a stable provision_detail_code.
func ProvisionDetailFromDeployError(err error) string {
	if err == nil {
		return "failed"
	}
	msg := err.Error()
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(msg, "timed out waiting") {
		return WaitCodeTimeout
	}
	if strings.Contains(msg, "CrashLoopBackOff") {
		return WaitCodeCrashLoop
	}
	if strings.Contains(msg, "CreateContainerConfigError") || strings.Contains(msg, "InvalidImageName") {
		return WaitCodeConfigError
	}
	return "failed"
}

// ComputeProvisionDetailCode picks the session-level detail code from replica progress and seed state.
func ComputeProvisionDetailCode(seedingStatus string, replicaStatus map[string][]PodWaitProgress, phase, deployFailDetail string) string {
	if deployFailDetail != "" {
		return deployFailDetail
	}
	for _, code := range []string{"failed", WaitCodeTimeout, WaitCodeCrashLoop, WaitCodeConfigError} {
		if anyReplicaHasWaitCode(replicaStatus, code) {
			return code
		}
	}
	if phase == "failed" {
		return "failed"
	}
	if seedingStatus == "seeding" {
		return "loading_sample_data"
	}
	if anyReplicaHasWaitCode(replicaStatus, WaitCodePullingImage) {
		return WaitCodePullingImage
	}
	return "layer_deploying"
}

// buildPodWaitProgressList maps listed pods to replica_status rows, adding pending placeholders when needed.
func buildPodWaitProgressList(pods []corev1.Pod, desiredReplicas int32, prefix []PodWaitProgress) []PodWaitProgress {
	if desiredReplicas <= 0 {
		desiredReplicas = 1
	}
	active := make([]corev1.Pod, 0, len(pods))
	for i := range pods {
		pod := pods[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		active = append(active, pod)
	}
	sort.Slice(active, func(i, j int) bool {
		return active[i].Name < active[j].Name
	})

	out := make([]PodWaitProgress, 0, len(prefix)+int(desiredReplicas))
	out = append(out, prefix...)
	base := len(prefix)
	for i := range active {
		pod := &active[i]
		out = append(out, PodWaitProgress{
			Ordinal:  base + i + 1,
			Ready:    podConditionReady(pod),
			WaitCode: waitCodeFromPod(pod),
		})
	}
	for i := len(active); i < int(desiredReplicas); i++ {
		out = append(out, PodWaitProgress{
			Ordinal:  base + i + 1,
			Ready:    false,
			WaitCode: WaitCodePending,
		})
	}
	return out
}

func anyReplicaHasWaitCode(replicaStatus map[string][]PodWaitProgress, code string) bool {
	for _, pods := range replicaStatus {
		for _, p := range pods {
			if p.WaitCode == code {
				return true
			}
		}
	}
	return false
}
