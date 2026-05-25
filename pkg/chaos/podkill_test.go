package chaos

import (
	"context"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPodKillScenario_Apply(t *testing.T) {
	ns := "cloud9-test"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-server-abc",
			Namespace: ns,
			Labels:    map[string]string{"app": "app-server"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewClientset(pod)

	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{{Type: "app-server", Name: "App"}},
	}
	scenario := &PodKillScenario{Client: client}

	if err := scenario.Apply(context.Background(), ns, design, "app-server"); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	_, err := client.CoreV1().Pods(ns).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected pod to be deleted")
	}
}
