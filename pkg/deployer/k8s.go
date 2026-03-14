package deployer

import (
	"context"
	"fmt"
	"log"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

// K8sDeployer is the real Kubernetes implementation of ComponentDeployer.
// clientset is the typed HTTP client for the K8s API.
// Every call on it (Create, Delete, Watch) maps to an HTTP request
// to the K8s API server. minikube's API server is what receives these.
type K8sDeployer struct {
	clientset kubernetes.Interface // Interface not *kubernetes.Clientset — allows test mocks
	nodeType  string
}

func NewK8sDeployer(clientset kubernetes.Interface, nodeType string) *K8sDeployer {
	return &K8sDeployer{clientset: clientset, nodeType: nodeType}
}


func (k *K8sDeployer) Deploy(ctx context.Context, cfg DeployConfig) error {
	if cfg.Node.Type != "db" {
		// Other component types (cache, app-server) implemented in Week 4
		log.Printf("K8sDeployer: skipping unsupported node type %q", cfg.Node.Type)
		return nil
	}

	// UNDERSTAND: namespace creation is idempotent-ish but we don't need to handle
	// AlreadyExists here: each deployment gets a unique namespace from the timestamp
	_, err := k.clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.Namespace},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create namespace: %w", err)
	}

	labels := map[string]string{"app": "postgres", "deployment": cfg.Namespace}

	// UNDERSTAND: resource requests/limits are required on GKE Autopilot.
	// Requests = guaranteed minimum. Limits = hard cap.
	// We set them equal here so Autopilot doesn't reject the pod.
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "postgres",
							Image: "postgres:15",
							Env: []corev1.EnvVar{
								{Name: "POSTGRES_DB", Value: "syscrack"},
								{Name: "POSTGRES_USER", Value: "syscrack"},
								{Name: "POSTGRES_PASSWORD", Value: "syscrack"},
							},
							Ports: []corev1.ContainerPort{{ContainerPort: 5432}},
							Resources: resources,
						},
					},
				},
			},
		},
	}
	_, err = k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create deployment: %w", err)
	}

	// UNDERSTAND: the Service is how other pods find Postgres by DNS name.
	// K8s gives it a stable internal DNS: {service-name}.{namespace}.svc.cluster.local
	// Without a Service, pods have no stable address — their IPs change on restart.
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: cfg.Namespace},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports:    []corev1.ServicePort{{Port: 5432, TargetPort: intstr.FromInt32(5432)}},
		},
	}
	_, err = k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	return k.waitForReady(ctx, cfg.Namespace)
}

func (k *K8sDeployer) waitForReady(ctx context.Context, namespace string) error {
	// UNDERSTAND: Watch gives us a stream of events (ADDED, MODIFIED, DELETED)
	// for pods matching our label selector. We block on this stream until
	// we see a pod with ConditionReady=True, or the context deadline fires.
	watcher, err := k.clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: "app=postgres",
	})
	if err != nil {
		return fmt.Errorf("watch pods: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed unexpectedly")
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return nil // pod is ready
				}
			}
		case <-ctx.Done():
			// UNDERSTAND: ctx.Done() closes when the context is cancelled or times out.
			// We return ctx.Err() not a custom error — callers use errors.Is(err, context.DeadlineExceeded)
			// to distinguish timeout from other failures.
			return fmt.Errorf("timed out waiting for postgres ready: %w", ctx.Err())
		}
	}
}

func (k *K8sDeployer) Teardown(ctx context.Context, namespace string) error {
	// UNDERSTAND: deleting the namespace cascades — K8s garbage collects everything
	// inside it (Deployments, Pods, Services, PVCs). We never enumerate and delete
	// individual resources. This is why namespace-per-deployment is the right isolation unit.
	deletePolicy := metav1.DeletePropagationForeground
	err := k.clientset.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	// UNDERSTAND: DeletePropagationForeground means the namespace object stays visible
	// (with a deletionTimestamp) until all contained resources are gone.
	// Background deletion returns immediately but resources linger briefly.
	// Foreground gives us a clean "it's really gone" guarantee.
	if err != nil {
		return err
	}
	return nil
}

func ptr(i int32) *int32 { return &i }

var _ ComponentDeployer = (*K8sDeployer)(nil)