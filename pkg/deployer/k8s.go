package deployer

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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

	// namespace creation is idempotent-ish. each deployment gets a unique namespace from the timestamp
	_, err := k.clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.Namespace},
	}, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace: %w", err)
	}

	// resource requests/limits are required on GKE Autopilot.
	// Requests = guaranteed minimum. Limits = hard cap.
	// We set them equal here so Autopilot doesn't reject the pod.
	resources := ResourceProfile(configString(cfg.Node.Config, "resourceProfile", "medium"))

	// replicas can be tuned per node type.
	replicas := int32(1)
	if r := configString(cfg.Node.Config, "replicas", "1"); r != "" {
		if n, err := strconv.Atoi(r); err == nil && n > 0 {
			replicas = int32(n)
		}
	}

	switch cfg.Node.Type {
	case "db", "mq":
		return k.deployFromRegistry(ctx, cfg, resources, replicas)
	case "cache":
		return k.deployCache(ctx, cfg, resources, replicas)
	case "app-server":
		return k.deployAppServer(ctx, cfg, resources, replicas)
	case "worker":
		return k.deployWorker(ctx, cfg, resources, replicas)
	case "lb":
		return k.deployLB(ctx, cfg, resources, replicas)
	default:
		log.Printf("K8sDeployer: skipping unsupported node type %q", cfg.Node.Type)
		return nil
	}
}

func (k *K8sDeployer) waitForReady(ctx context.Context, namespace string, appLabel string) error {
	// Watch gives us a stream of events (ADDED, MODIFIED, DELETED)
	// for pods matching our label selector. We block on this stream until
	// we see a pod with ConditionReady=True, or the context deadline fires.
	watcher, err := k.clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: "deployment=" + namespace + ",app=" + appLabel,
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
			return fmt.Errorf("timed out waiting for %s ready: %w", appLabel, ctx.Err())
		}
	}
}

func (k *K8sDeployer) deployCache(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) error {
	envVars := DeriveEnvVars(cfg.Node, cfg.Design)

	if requiresProxy(cfg.Node) {
		// Proxy strategies deploy two workloads from the single canvas cache node:
		// 1) internal Redis (simple redis:7)
		// 2) caching-proxy (HTTP->Redis->DB wrapper)
		redisLabels := map[string]string{"app": "redis-internal", "deployment": cfg.Namespace}
		internalRedisDep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "redis-internal", Namespace: cfg.Namespace},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr(replicas),
				Selector: &metav1.LabelSelector{MatchLabels: redisLabels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: redisLabels},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:      "redis",
								Image:     "redis:7",
								Ports:     []corev1.ContainerPort{{ContainerPort: 6379}},
								Resources: resources,
							},
						},
					},
				},
			},
		}
		_, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, internalRedisDep, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create internal redis deployment: %w", err)
		}

		internalRedisSvc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "redis-internal", Namespace: cfg.Namespace},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: redisLabels,
				Ports:    []corev1.ServicePort{{Port: 6379, TargetPort: intstr.FromInt32(6379)}},
			},
		}
		_, err = k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, internalRedisSvc, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create internal redis service: %w", err)
		}

		// Ensure the Redis backend is ready before the proxy starts.
		if err := k.waitForReady(ctx, cfg.Namespace, "redis-internal"); err != nil {
			return err
		}

		proxyLabels := map[string]string{"app": "cache-proxy", "deployment": cfg.Namespace}
		proxyStrategy := configString(cfg.Node.Config, "strategy", "write-through")
		proxyEnv := append(
			envMapToK8s(envVars),
			corev1.EnvVar{Name: "REDIS_HOST", Value: "redis-internal"},
			corev1.EnvVar{Name: "REDIS_PORT", Value: "6379"},
			corev1.EnvVar{Name: "PORT", Value: "8081"},
		)

		proxyDep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "cache-proxy", Namespace: cfg.Namespace},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr(replicas),
				Selector: &metav1.LabelSelector{MatchLabels: proxyLabels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: proxyLabels},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:      "cache-proxy",
								Image:     cacheImage(proxyStrategy),
								Ports:     []corev1.ContainerPort{{ContainerPort: 8081}},
								Resources: resources,
								Env:       proxyEnv,
							},
						},
					},
				},
			},
		}
		_, err = k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, proxyDep, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create cache proxy deployment: %w", err)
		}

		proxySvc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: cfg.Namespace},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: proxyLabels,
				Ports:    []corev1.ServicePort{{Port: 8081, TargetPort: intstr.FromInt32(8081)}},
			},
		}
		_, err = k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, proxySvc, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create cache proxy service: %w", err)
		}

		return k.waitForReady(ctx, cfg.Namespace, "cache-proxy")
	}

	// Cache-aside: single Redis pod/service.
	labels := map[string]string{"app": "redis", "deployment": cfg.Namespace}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:      "redis",
							Image:     cacheImage("cache-aside"),
							Ports:     []corev1.ContainerPort{{ContainerPort: 6379}},
							Resources: resources,
							Env:       envMapToK8s(envVars),
						},
					},
				},
			},
		},
	}

	_, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create deployment: %w", err)
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: cfg.Namespace},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports:    []corev1.ServicePort{{Port: 6379, TargetPort: intstr.FromInt32(6379)}},
		},
	}

	_, err = k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	return k.waitForReady(ctx, cfg.Namespace, "redis")
}

func (k *K8sDeployer) deployAppServer(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) error {
	envVars := DeriveEnvVars(cfg.Node, cfg.Design)

	// Image is a placeholder for now — Week 6 replaces with pre-built template images.
	labels := map[string]string{"app": "app-server", "deployment": cfg.Namespace}

	// These annotations are how Prometheus discovers which pods to scrape.
	appAnnotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   "8080",
		"prometheus.io/path":   "/metrics",
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: appAnnotations},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:      "app-server",
						Image:     appServerImage(cfg.Design.Template),
						Ports:     []corev1.ContainerPort{{ContainerPort: 8080}},
						Resources: resources,
						// UNDERSTAND: we use HTTP readiness to make PodReady reflect /health.
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromInt32(8080)},
							},
							InitialDelaySeconds: 3,
							PeriodSeconds:       5,
						},
						Env: envMapToK8s(envVars),
					}},
				},
			},
		},
	}
	_, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create deployment: %w", err)
	}

	svcApp := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: cfg.Namespace},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports:    []corev1.ServicePort{{Port: 8080, TargetPort: intstr.FromInt32(8080)}},
		},
	}
	_, err = k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svcApp, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	return k.waitForReady(ctx, cfg.Namespace, "app-server")
}

func (k *K8sDeployer) deployWorker(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) error {
	log.Printf("K8sDeployer: skipping unsupported node type %q (stub in Stream 2)", cfg.Node.Type)
	return nil
}

func (k *K8sDeployer) deployLB(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) error {
	log.Printf("K8sDeployer: skipping unsupported node type %q (stub in Stream 2)", cfg.Node.Type)
	return nil
}

func (k *K8sDeployer) deployFromRegistry(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) error {
	spec, ok := componentRegistry[cfg.Node.Type]
	if !ok {
		log.Printf("K8sDeployer: skipping unsupported node type %q", cfg.Node.Type)
		return nil
	}
	if len(spec.Ports) == 0 {
		return fmt.Errorf("registry spec for %q must define at least one port", cfg.Node.Type)
	}

	envVars := DeriveEnvVars(cfg.Node, cfg.Design)

	// Merge default env and derived env (derived wins on collisions).
	allEnv := make(map[string]string, len(spec.DefaultEnv)+len(envVars))
	for k, v := range spec.DefaultEnv {
		allEnv[k] = v
	}
	for k, v := range envVars {
		allEnv[k] = v
	}

	labels := map[string]string{"app": spec.AppLabel, "deployment": cfg.Namespace}

	// TCP readiness is a lightweight readiness gate for simple infra components.
	probe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(spec.Ports[0])},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
	}

	ports := make([]corev1.ContainerPort, len(spec.Ports))
	for i, p := range spec.Ports {
		ports[i] = corev1.ContainerPort{ContainerPort: p}
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: spec.AppLabel, Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: spec.Annotations},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            spec.AppLabel,
						Image:           spec.Image,
						Ports:           ports,
						Resources:       resources,
						Env:             envMapToK8s(allEnv),
						ReadinessProbe: probe,
					}},
				},
			},
		},
	}

	_, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create deployment: %w", err)
	}

	if spec.NeedsService {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: spec.ServiceName, Namespace: cfg.Namespace},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: labels,
				Ports: []corev1.ServicePort{
					{Port: spec.ServicePort, TargetPort: intstr.FromInt32(spec.Ports[0])},
				},
			},
		}
		_, err := k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svc, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create service: %w", err)
		}
	}

	return k.waitForReady(ctx, cfg.Namespace, spec.AppLabel)
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

func appServerImage(template string) string {
	registry := os.Getenv("TEMPLATE_IMAGE_REGISTRY")
	if registry == "" {
		registry = "europe-west1-docker.pkg.dev/ganymede-cloud/cloud9-images"
	}
	switch template {
	case "url-shortener":
		// Built/pushed as part of Week 8.
		return registry + "/url-shortener:v5"
	default:
		// Fallback so other templates still deploy in Week 6.
		return "nginx:latest"
	}
}

func cacheImage(strategy string) string {
	registry := os.Getenv("TEMPLATE_IMAGE_REGISTRY")
	if registry == "" {
		registry = "europe-west1-docker.pkg.dev/ganymede-cloud/cloud9-images"
	}
	// UNDERSTAND: cache-aside uses plain Redis; proxy strategies use a custom Go service.
	switch strategy {
	case "write-through", "write-behind", "read-through":
		return registry + "/caching-proxy:v1"
	default:
		return "redis:7"
	}
}

func envMapToK8s(m map[string]string) []corev1.EnvVar {
	if len(m) == 0 {
		return nil
	}
	var vars []corev1.EnvVar
	for k, v := range m {
		vars = append(vars, corev1.EnvVar{Name: k, Value: v})
	}
	return vars
}

var _ ComponentDeployer = (*K8sDeployer)(nil)
