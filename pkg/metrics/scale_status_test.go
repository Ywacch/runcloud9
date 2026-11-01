package metrics

import (
	"strings"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func hpaForDeployment(name string, desired int32) autoscalingv2.HorizontalPodAutoscaler {
	return autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				Kind: "Deployment",
				Name: name,
			},
		},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{
			DesiredReplicas: desired,
		},
	}
}

func TestScaleStatusForApp_NoHPA(t *testing.T) {
	t.Parallel()
	if got := scaleStatusForApp(nil, "app-server", 1); got != nil {
		t.Fatalf("want nil, got %+v", got)
	}
}

func TestScaleStatusForApp_DesiredAndReady(t *testing.T) {
	t.Parallel()
	hpas := []autoscalingv2.HorizontalPodAutoscaler{hpaForDeployment("app-server", 2)}
	got := scaleStatusForApp(hpas, "app-server", 1)
	if got == nil || got.Desired != 2 || got.Ready != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestScaleStatusForApp_UnreportedDesired(t *testing.T) {
	t.Parallel()
	hpas := []autoscalingv2.HorizontalPodAutoscaler{hpaForDeployment("app-server", 0)}
	if got := scaleStatusForApp(hpas, "app-server", 1); got != nil {
		t.Fatalf("want nil, got %+v", got)
	}
}

func TestScaleStatusForApp_IgnoresOtherApps(t *testing.T) {
	t.Parallel()
	hpas := []autoscalingv2.HorizontalPodAutoscaler{hpaForDeployment("worker", 3)}
	if got := scaleStatusForApp(hpas, "app-server", 1); got != nil {
		t.Fatalf("want nil, got %+v", got)
	}
}

func TestDiffScaleEvents_FirstSampleStoresOnly(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	events, stored := diffScaleEvents(nil, map[string]int32{"app-server": 1}, now)
	if len(events) != 0 {
		t.Fatalf("events: %+v", events)
	}
	if stored["app-server"] != 1 {
		t.Fatalf("stored: %+v", stored)
	}
}

func TestDiffScaleEvents_ChangeEmitsOnce(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	prev := map[string]int32{"app-server": 1}
	events, stored := diffScaleEvents(prev, map[string]int32{"app-server": 2}, now)
	if len(events) != 1 {
		t.Fatalf("events: %+v", events)
	}
	ev := events[0]
	if ev.Kind != "scale" || ev.Source != "autoscaler" || ev.Component != "app-server" {
		t.Fatalf("event: %+v", ev)
	}
	if ev.Detail["from"] != "1" || ev.Detail["to"] != "2" || ev.Detail["reason"] != "cpu" || ev.Detail["fidelity"] != "real" {
		t.Fatalf("detail: %+v", ev.Detail)
	}
	if !strings.Contains(ev.Description, "60 seconds") {
		t.Fatalf("description: %q", ev.Description)
	}
	if stored["app-server"] != 2 {
		t.Fatalf("stored: %+v", stored)
	}
	again, stored2 := diffScaleEvents(stored, map[string]int32{"app-server": 2}, now)
	if len(again) != 0 {
		t.Fatalf("second tick events: %+v", again)
	}
	if stored2["app-server"] != 2 {
		t.Fatalf("stored2: %+v", stored2)
	}
}
