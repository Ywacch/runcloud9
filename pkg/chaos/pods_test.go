package chaos

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDeleteRunningPodsForAppLabel(t *testing.T) {
	t.Parallel()
	ns := "cloud9-pods"
	client := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-1",
				Namespace: ns,
				Labels:    map[string]string{"app": appServerAppLabel},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-2",
				Namespace: ns,
				Labels:    map[string]string{"app": appServerAppLabel},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)
	if err := deleteRunningPodsForAppLabel(context.Background(), client, ns, appServerAppLabel); err != nil {
		t.Fatalf("deleteRunningPodsForAppLabel: %v", err)
	}
	list, err := client.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{
		LabelSelector: labelSelector(appServerAppLabel),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("expected no app-server pods after delete, got %d", len(list.Items))
	}
}
