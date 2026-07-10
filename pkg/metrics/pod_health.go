package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	rulePodRestart = "pod_restart"
	rulePodOOM     = "pod_oom"
)

type podHealthTracker struct {
	lastRestart map[types.UID]int32
}

func (c *MetricsCollector) podHealthTracker(namespace string) *podHealthTracker {
	c.podHealthMu.Lock()
	defer c.podHealthMu.Unlock()
	if c.podHealth == nil {
		c.podHealth = make(map[string]*podHealthTracker)
	}
	t := c.podHealth[namespace]
	if t == nil {
		t = &podHealthTracker{lastRestart: make(map[types.UID]int32)}
		c.podHealth[namespace] = t
	}
	return t
}

func componentKeyForAppLabel(app string) string {
	switch app {
	case "app-server":
		return "app-server"
	case "worker":
		return "worker"
	case "postgres":
		return "db"
	case "rabbitmq":
		return "mq"
	case "lb":
		return "lb"
	case "redis", "redis-internal", "cache-proxy":
		return "cache"
	default:
		return ""
	}
}

func listDeploymentPods(ctx context.Context, clientset kubernetes.Interface, namespace string) ([]corev1.Pod, error) {
	if clientset == nil || namespace == "" {
		return nil, nil
	}
	list, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("deployment=%s", namespace),
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func groupPodsByComponent(pods []corev1.Pod) map[string][]corev1.Pod {
	out := make(map[string][]corev1.Pod)
	for _, pod := range pods {
		app := pod.Labels["app"]
		key := componentKeyForAppLabel(app)
		if key == "" {
			continue
		}
		out[key] = append(out[key], pod)
	}
	return out
}

// podContainerSignals returns the max restart count and dominant waiting/termination reasons.
func podContainerSignals(pod corev1.Pod) (restartCount int32, waitingReason, termReason string, exitCode int32) {
	scan := func(css []corev1.ContainerStatus) {
		for _, cs := range css {
			if cs.RestartCount > restartCount {
				restartCount = cs.RestartCount
			}
			if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
				waitingReason = cs.State.Waiting.Reason
			}
			if cs.LastTerminationState.Terminated != nil {
				term := cs.LastTerminationState.Terminated
				if term.Reason != "" {
					termReason = term.Reason
				}
				exitCode = term.ExitCode
			}
		}
	}
	scan(pod.Status.InitContainerStatuses)
	scan(pod.Status.ContainerStatuses)
	return restartCount, waitingReason, termReason, exitCode
}

func buildRuntimeHealth(pods []corev1.Pod) *RuntimeHealth {
	if len(pods) == 0 {
		return nil
	}
	rh := &RuntimeHealth{Pods: make([]PodRuntimeHealth, 0, len(pods))}
	for _, pod := range pods {
		restarts, waiting, term, exit := podContainerSignals(pod)
		ready := podReadyStatus(pod)
		if !ready {
			rh.UnreadyCount++
		}
		if waiting == "CrashLoopBackOff" {
			rh.Restarting = true
		}
		rh.Pods = append(rh.Pods, PodRuntimeHealth{
			Name:                  pod.Name,
			Ready:                 ready,
			RestartCount:          restarts,
			WaitingReason:         waiting,
			LastTerminationReason: term,
			ExitCode:              exit,
		})
	}
	return rh
}

func buildCrashContext(component string, dm *DeploymentMetrics, podName, termReason, waitingReason string, exitCode int32) *CrashContext {
	if dm == nil {
		return &CrashContext{Pod: podName, TerminationReason: termReason, WaitingReason: waitingReason, ExitCode: exitCode}
	}
	cm := dm.Components[component]
	ctx := &CrashContext{
		Pod:               podName,
		TerminationReason: termReason,
		WaitingReason:     waitingReason,
		ExitCode:          exitCode,
		ErrorRate:         cm.ErrorRate,
		MemoryLimitBytes:  cm.MemoryLimit,
	}
	if cm.CPULimit > 0 && cm.CPUUsage > 0 {
		ctx.CPUPercent = 100 * cm.CPUUsage / cm.CPULimit
	}
	if cm.MemoryLimit > 0 && cm.MemoryBytes > 0 {
		ctx.MemPercent = 100 * float64(cm.MemoryBytes) / float64(cm.MemoryLimit)
	}
	switch component {
	case "lb":
		ctx.ActiveConnections = cm.ActiveConnections
	case "cache":
		ctx.RedisRejectedRate = cm.RedisRejectedRate
		ctx.RedisOpsPerSec = cm.RedisOpsPerSec
	case "mq", "worker":
		ctx.QueueDepth = cm.QueueDepth
	}
	return ctx
}

func (t *podHealthTracker) detectRestartEvent(
	component string,
	pod corev1.Pod,
	dm *DeploymentMetrics,
	now time.Time,
) *Event {
	if t == nil {
		return nil
	}
	restarts, waiting, termReason, exitCode := podContainerSignals(pod)
	prev, seen := t.lastRestart[pod.UID]
	if !seen {
		t.lastRestart[pod.UID] = restarts
		return nil
	}
	if restarts <= prev {
		return nil
	}
	t.lastRestart[pod.UID] = restarts

	ruleID := rulePodRestart
	title := fmt.Sprintf("%s restarted", pod.Name)
	desc := fmt.Sprintf("Container restart count increased to %d", restarts)
	if strings.EqualFold(termReason, "OOMKilled") {
		ruleID = rulePodOOM
		title = fmt.Sprintf("%s OOMKilled", pod.Name)
		desc = "Container exceeded its memory limit and was terminated"
	} else if waiting == "CrashLoopBackOff" {
		desc = fmt.Sprintf("Pod in CrashLoopBackOff after %d restarts", restarts)
	}

	ev := Event{
		Timestamp:    now,
		Type:         "action",
		Source:       "runtime",
		Component:    component,
		RuleID:       ruleID,
		Title:        title,
		Description:  desc,
		CrashContext: buildCrashContext(component, dm, pod.Name, termReason, waiting, exitCode),
	}
	var n NarrativeOutput
	if ruleID == rulePodOOM {
		n = narratePodOOM(ev.CrashContext)
	} else {
		n = narratePodRestart(ev.CrashContext)
	}
	ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
	return &ev
}

// attachRuntimeHealth lists deployment pods, attaches runtimeHealth to components, and emits restart events.
func (c *MetricsCollector) attachRuntimeHealth(
	ctx context.Context,
	namespace string,
	dm *DeploymentMetrics,
) []Event {
	if c == nil || c.clientset == nil || dm == nil || namespace == "" {
		return nil
	}
	pods, err := listDeploymentPods(ctx, c.clientset, namespace)
	if err != nil || len(pods) == 0 {
		return nil
	}

	tracker := c.podHealthTracker(namespace)
	byComponent := groupPodsByComponent(pods)
	now := time.Now()
	var events []Event

	for component, componentPods := range byComponent {
		if rh := buildRuntimeHealth(componentPods); rh != nil {
			if cm, ok := dm.Components[component]; ok {
				cm.RuntimeHealth = rh
				dm.Components[component] = cm
			}
		}
		for _, pod := range componentPods {
			if ev := tracker.detectRestartEvent(component, pod, dm, now); ev != nil {
				events = append(events, *ev)
			}
		}
	}
	return events
}
