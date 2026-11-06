package clusterstate

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTrimPod_DropsEnvKeepsResources(t *testing.T) {
	cpuLimit := resource.MustParse("500m")
	in := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app",
			Namespace: "ns-a",
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kube-controller-manager"},
			},
			Annotations: map[string]string{"note": "drop-me"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "app",
				Env:  []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{corev1.ResourceCPU: cpuLimit},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	out := trimPod(in)
	if out == nil {
		t.Fatal("nil pod")
	}
	if len(out.ManagedFields) != 0 {
		t.Fatalf("managedFields = %v, want empty", out.ManagedFields)
	}
	if len(out.Annotations) != 0 {
		t.Fatalf("annotations = %v, want empty", out.Annotations)
	}
	if len(out.Spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(out.Spec.Containers))
	}
	if len(out.Spec.Containers[0].Env) != 0 {
		t.Fatalf("env = %v, want dropped", out.Spec.Containers[0].Env)
	}
	gotLimit := out.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU]
	if gotLimit.Cmp(cpuLimit) != 0 {
		t.Fatalf("cpu limit = %v, want %v", gotLimit, cpuLimit)
	}
	if len(out.Status.Conditions) != 1 || out.Status.Conditions[0].Type != corev1.PodReady {
		t.Fatalf("conditions = %v, want Ready intact", out.Status.Conditions)
	}
}

func TestTrimDeployment_KeepsReplicas(t *testing.T) {
	replicas := int32(3)
	in := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: "ns-a"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "server"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "x"}}},
			},
		},
	}
	out := trimDeployment(in)
	if out.Spec.Replicas == nil || *out.Spec.Replicas != replicas {
		t.Fatalf("replicas = %v, want %d", out.Spec.Replicas, replicas)
	}
	if out.Spec.Template.Spec.Containers != nil {
		t.Fatalf("template body = %+v, want empty", out.Spec.Template)
	}
}
