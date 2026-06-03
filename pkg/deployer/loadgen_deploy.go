package deployer

import (
	"context"
	"fmt"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	"github.com/ywacch/cloud9/pkg/templates"
)

const (
	loadgenAppLabel   = "loadgen"
	loadgenPort       = 9100
	loadgenDeployName = "loadgen"
	loadgenService    = "loadgen"
)

// DeployLoadgen creates the per-namespace loadgen Deployment and Service.
// userTier selects CPU/memory ("explorer" default, "engineer" for higher limits).
func DeployLoadgen(ctx context.Context, clientset kubernetes.Interface, namespace, userTier string) error {
	if clientset == nil {
		return fmt.Errorf("deploy loadgen: kubernetes client is nil")
	}
	if namespace == "" {
		return fmt.Errorf("deploy loadgen: namespace is required")
	}

	resources := loadgenResources(userTier)
	image := loadgenImage()
	labels := map[string]string{"app": loadgenAppLabel, "deployment": namespace}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: loadgenDeployName, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "loadgen",
							Image:           image,
							ImagePullPolicy: corev1.PullAlways,
							Ports:           []corev1.ContainerPort{{ContainerPort: loadgenPort}},
							Resources:       resources,
						},
					},
				},
			},
		},
	}

	_, err := clientset.AppsV1().Deployments(namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create loadgen deployment: %w", err)
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: loadgenService, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Port:       loadgenPort,
				TargetPort: intstr.FromInt32(loadgenPort),
			}},
		},
	}
	_, err = clientset.CoreV1().Services(namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create loadgen service: %w", err)
	}

	return nil
}

func loadgenImage() string {
	registry := os.Getenv("TEMPLATE_IMAGE_REGISTRY")
	if registry == "" {
		registry = "us-central1-docker.pkg.dev/runcloud9/cloud9-images"
	}
	return registry + "/loadgen:" + templates.ImageTag("loadgen")
}

func loadgenResources(userTier string) corev1.ResourceRequirements {
	switch userTier {
	case "engineer", "architect":
		return corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		}
	default:
		return corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("250m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("250m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		}
	}
}
