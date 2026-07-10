package metrics

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestComponentKeyForAppLabel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		app  string
		want string
	}{
		{"app-server", "app-server"},
		{"worker", "worker"},
		{"postgres", "db"},
		{"rabbitmq", "mq"},
		{"lb", "lb"},
		{"redis", "cache"},
		{"redis-internal", "cache"},
		{"cache-proxy", "cache"},
		{"loadgen", ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.app, func(t *testing.T) {
			t.Parallel()
			if got := componentKeyForAppLabel(tc.app); got != tc.want {
				t.Fatalf("componentKeyForAppLabel(%q) = %q, want %q", tc.app, got, tc.want)
			}
		})
	}
}

func TestPodContainerSignals_OOMKilled(t *testing.T) {
	t.Parallel()
	pod := corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "redis",
				RestartCount: 2,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "OOMKilled",
						ExitCode: 137,
					},
				},
			}},
		},
	}
	restarts, waiting, term, exit := podContainerSignals(pod)
	if restarts != 2 {
		t.Fatalf("restarts = %d, want 2", restarts)
	}
	if waiting != "CrashLoopBackOff" {
		t.Fatalf("waiting = %q, want CrashLoopBackOff", waiting)
	}
	if term != "OOMKilled" {
		t.Fatalf("term = %q, want OOMKilled", term)
	}
	if exit != 137 {
		t.Fatalf("exit = %d, want 137", exit)
	}
}

func TestBuildCrashContext_cacheComponent(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"cache": {
				Component:         "cache",
				CPUUsage:          0.1,
				CPULimit:          0.25,
				MemoryBytes:       200 << 20,
				MemoryLimit:       256 << 20,
				RedisRejectedRate: 42,
				RedisOpsPerSec:    21000,
			},
		},
	}
	ctx := buildCrashContext("cache", dm, "redis-internal-abc", "OOMKilled", "CrashLoopBackOff", 137)
	if ctx.Pod != "redis-internal-abc" {
		t.Fatalf("pod: %+v", ctx)
	}
	if ctx.TerminationReason != "OOMKilled" {
		t.Fatalf("termination: %+v", ctx)
	}
	if ctx.MemPercent < 75 || ctx.MemPercent > 85 {
		t.Fatalf("memPercent = %v, want ~78", ctx.MemPercent)
	}
	if ctx.RedisRejectedRate != 42 || ctx.RedisOpsPerSec != 21000 {
		t.Fatalf("redis fields: %+v", ctx)
	}
}

func TestPodHealthTracker_detectRestartEvent(t *testing.T) {
	t.Parallel()
	tracker := &podHealthTracker{lastRestart: make(map[types.UID]int32)}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redis-internal-abc",
			UID:  types.UID("uid-redis-1"),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				RestartCount: 1,
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1},
				},
			}},
		},
	}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{
		"cache": {Component: "cache", MemoryLimit: 256 << 20, MemoryBytes: 240 << 20},
	}}
	now := time.Now()

	if ev := tracker.detectRestartEvent("cache", pod, dm, now); ev != nil {
		t.Fatalf("first pass should seed without event, got %+v", ev)
	}

	pod.Status.ContainerStatuses[0].RestartCount = 2
	ev := tracker.detectRestartEvent("cache", pod, dm, now)
	if ev == nil {
		t.Fatal("expected restart event on second pass")
	}
	if ev.RuleID != rulePodRestart {
		t.Fatalf("ruleId = %q, want %q", ev.RuleID, rulePodRestart)
	}
	if ev.Source != "runtime" || ev.Component != "cache" {
		t.Fatalf("event meta: %+v", ev)
	}
	if ev.CrashContext == nil || ev.CrashContext.Pod != "redis-internal-abc" {
		t.Fatalf("crashContext: %+v", ev.CrashContext)
	}
	if ev.Narrative == "" {
		t.Fatal("expected narrative on restart event")
	}
}

func TestPodHealthTracker_detectOOMEvent(t *testing.T) {
	t.Parallel()
	tracker := &podHealthTracker{lastRestart: make(map[types.UID]int32)}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redis-internal-oom",
			UID:  types.UID("uid-redis-oom"),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				RestartCount: 1,
			}},
		},
	}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{
		"cache": {Component: "cache", MemoryLimit: 256 << 20, MemoryBytes: 250 << 20},
	}}
	now := time.Now()
	_ = tracker.detectRestartEvent("cache", pod, dm, now)

	pod.Status.ContainerStatuses[0].RestartCount = 2
	pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137},
	}
	ev := tracker.detectRestartEvent("cache", pod, dm, now)
	if ev == nil {
		t.Fatal("expected OOM event")
	}
	if ev.RuleID != rulePodOOM {
		t.Fatalf("ruleId = %q, want %q", ev.RuleID, rulePodOOM)
	}
	if ev.CrashContext == nil || ev.CrashContext.TerminationReason != "OOMKilled" {
		t.Fatalf("crashContext: %+v", ev.CrashContext)
	}
	if ev.CrashContext.MemPercent < 90 {
		t.Fatalf("memPercent = %v, want high utilization", ev.CrashContext.MemPercent)
	}
}

func TestAttachRuntimeHealth_populatesComponentAndEvents(t *testing.T) {
	t.Parallel()
	ns := "cloud9-test-ns"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-internal-xyz",
			Namespace: ns,
			Labels: map[string]string{
				"app":        "redis-internal",
				"deployment": ns,
			},
			UID: types.UID("uid-xyz"),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "redis",
				Ready:        true,
				RestartCount: 0,
			}},
		},
	}
	clientset := fake.NewSimpleClientset(pod)
	c := NewMetricsCollector("", nil, clientset, nil, nil)
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"cache": {Component: "cache"},
		},
	}

	events := c.attachRuntimeHealth(context.Background(), ns, dm)
	if len(events) != 0 {
		t.Fatalf("first collect: want 0 events, got %d", len(events))
	}
	cache := dm.Components["cache"]
	if cache.RuntimeHealth == nil || len(cache.RuntimeHealth.Pods) != 1 {
		t.Fatalf("runtimeHealth: %+v", cache.RuntimeHealth)
	}
	if !cache.RuntimeHealth.Pods[0].Ready || cache.RuntimeHealth.Pods[0].Name != "redis-internal-xyz" {
		t.Fatalf("pod row: %+v", cache.RuntimeHealth.Pods[0])
	}

	pod.Status.ContainerStatuses[0].RestartCount = 1
	pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137},
	}
	latest, err := clientset.CoreV1().Pods(ns).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	latest.Status = pod.Status
	if _, err := clientset.CoreV1().Pods(ns).Update(context.Background(), latest, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update pod: %v", err)
	}

	dm.Components["cache"] = ComponentMetrics{
		Component:   "cache",
		MemoryLimit: 256 << 20,
		MemoryBytes: 250 << 20,
	}
	events = c.attachRuntimeHealth(context.Background(), ns, dm)
	if len(events) != 1 {
		t.Fatalf("second collect: want 1 event, got %d (%+v)", len(events), events)
	}
	if events[0].RuleID != rulePodOOM {
		t.Fatalf("event: %+v", events[0])
	}
}

func TestBuildRuntimeHealth_unreadyAndCrashLoop(t *testing.T) {
	t.Parallel()
	pods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "cache-proxy-1"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				RestartCount: 4,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
			}},
		},
	}}
	rh := buildRuntimeHealth(pods)
	if rh == nil || rh.UnreadyCount != 1 || !rh.Restarting {
		t.Fatalf("runtime health: %+v", rh)
	}
}
