package deployer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	"github.com/ywacch/cloud9/pkg/templates"
	"gopkg.in/yaml.v3"
)

// Provisioning waits: fail fast on clearly unrecoverable container config, debounce CrashLoopBackOff
// so transient blips do not abort before Kubernetes has retried, but broken apps do not burn the
// full orchestration context (~5m on k8s). Image pull backoffs are left to the parent context.
const (
	waitReadyCrashLoopDebounce          = 45 * time.Second
	waitReadyRestartFailThreshold int32 = 3
)

// templateHTTPProbe builds an HTTP GET probe at path on port with the given timings.
func templateHTTPProbe(path string, port, initialDelaySeconds, periodSeconds, failureThreshold, timeoutSeconds int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(port)},
		},
		InitialDelaySeconds: initialDelaySeconds,
		PeriodSeconds:       periodSeconds,
		TimeoutSeconds:      timeoutSeconds,
		FailureThreshold:    failureThreshold,
	}
}

// templateHTTPReadinessProbe returns GET /health readiness for template app containers.
// /health commonly touches Postgres/cache/MQ; default probe timeout is only 1s — under load that
// causes flaky NotReady. Caller picks delays appropriate for startup vs steady state.
// UNDERSTAND: kept for worker containers whose images do not (yet) serve the split
// /health/live and /health/ready endpoints; app-server and cache-proxy use the split probes below.
func templateHTTPReadinessProbe(port, initialDelaySeconds, periodSeconds, failureThreshold, timeoutSeconds int32) *corev1.Probe {
	return templateHTTPProbe("/health", port, initialDelaySeconds, periodSeconds, failureThreshold, timeoutSeconds)
}

// templateReadyProbe returns GET /health/ready readiness. Unlike /health it does not chain
// downstream dependency checks, so a slow or briefly-unavailable Redis/DB does not flip every
// pod NotReady at once and drain the load balancer of backends (the readiness-cascade fix).
func templateReadyProbe(port, initialDelaySeconds, periodSeconds, failureThreshold, timeoutSeconds int32) *corev1.Probe {
	return templateHTTPProbe("/health/ready", port, initialDelaySeconds, periodSeconds, failureThreshold, timeoutSeconds)
}

// templateLiveProbe returns GET /health/live liveness. The endpoint performs no downstream
// calls, so thresholds are conservative: liveness should fail (and restart the container) only
// when the process itself is wedged, never because a dependency is momentarily slow.
func templateLiveProbe(port int32) *corev1.Probe {
	return templateHTTPProbe("/health/live", port, 10, 10, 6, 2)
}

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
	// Apply namespace egress guardrails before any workloads are created.
	// Baseline is deny-all egress plus minimal allow rules (DNS + same-namespace traffic).
	if err := k.ensureNamespaceNetworkPolicies(ctx, cfg.Namespace); err != nil {
		return fmt.Errorf("ensure namespace network policies: %w", err)
	}

	// resource requests/limits are required on GKE Autopilot.
	// Requests = guaranteed minimum. Limits = hard cap.
	// We set them equal here so Autopilot doesn't reject the pod.
	resources := ResolveNodeResources(cfg.Node.Type, cfg.Node.Config)

	// replicas can be tuned per node type (app-server bounds enforced in ValidateRuntimeContract).
	replicas := parseReplicaCount(cfg.Node.Config, 1)

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

func terminalContainerWaitingReason(reason string) bool {
	switch reason {
	case "CreateContainerConfigError", "InvalidImageName":
		return true
	default:
		return false
	}
}

// detectWaitReadyFailure returns a non-nil error if the pod will not become ready without intervention.
// crashLoopSince tracks the first time we saw CrashLoopBackOff for a pod UID (cleared when it clears).
func detectWaitReadyFailure(pod *corev1.Pod, appLabel string, crashLoopSince map[types.UID]time.Time) error {
	var crashLooping bool
	scan := func(css []corev1.ContainerStatus) error {
		for _, cs := range css {
			if w := cs.State.Waiting; w != nil {
				if terminalContainerWaitingReason(w.Reason) {
					return fmt.Errorf("pod %s/%s container %s: %s — %s",
						pod.Namespace, pod.Name, cs.Name, w.Reason, strings.TrimSpace(w.Message))
				}
				if w.Reason == "CrashLoopBackOff" {
					crashLooping = true
					if cs.RestartCount >= waitReadyRestartFailThreshold {
						return fmt.Errorf("pod %s/%s container %s in CrashLoopBackOff after %d restarts: %s",
							pod.Namespace, pod.Name, cs.Name, cs.RestartCount, strings.TrimSpace(w.Message))
					}
				}
			}
		}
		return nil
	}
	if err := scan(pod.Status.InitContainerStatuses); err != nil {
		return err
	}
	if err := scan(pod.Status.ContainerStatuses); err != nil {
		return err
	}
	if crashLooping {
		if _, ok := crashLoopSince[pod.UID]; !ok {
			crashLoopSince[pod.UID] = time.Now()
			return nil
		}
		if time.Since(crashLoopSince[pod.UID]) >= waitReadyCrashLoopDebounce {
			return fmt.Errorf("pod %s/%s: CrashLoopBackOff persisted %v waiting for %s (image or app misconfiguration likely)",
				pod.Namespace, pod.Name, waitReadyCrashLoopDebounce, appLabel)
		}
		return nil
	}
	delete(crashLoopSince, pod.UID)
	return nil
}

func podConditionReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (k *K8sDeployer) waitForReady(ctx context.Context, namespace string, appLabel string, desiredReplicas int32) error {
	if desiredReplicas <= 0 {
		desiredReplicas = 1
	}

	labelSelector := "deployment=" + namespace + ",app=" + appLabel
	readyPods := make(map[types.UID]struct{})

	existing, err := k.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("list pods for %s: %w", appLabel, err)
	}
	for i := range existing.Items {
		if podConditionReady(&existing.Items[i]) {
			readyPods[existing.Items[i].UID] = struct{}{}
		}
	}
	if int32(len(readyPods)) >= desiredReplicas {
		return nil
	}

	watcher, err := k.clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("watch pods: %w", err)
	}
	defer watcher.Stop()

	crashLoopSince := make(map[types.UID]time.Time)

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
			if pod.DeletionTimestamp != nil {
				delete(readyPods, pod.UID)
				continue
			}
			if podConditionReady(pod) {
				readyPods[pod.UID] = struct{}{}
				if int32(len(readyPods)) >= desiredReplicas {
					return nil
				}
			} else {
				delete(readyPods, pod.UID)
			}
			if err := detectWaitReadyFailure(pod, appLabel, crashLoopSince); err != nil {
				return fmt.Errorf("waiting for %s ready: %w", appLabel, err)
			}
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %d %s pod(s) ready (have %d): %w",
				desiredReplicas, appLabel, len(readyPods), ctx.Err())
		}
	}
}

func (k *K8sDeployer) deployCache(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) error {
	envVars := DeriveEnvVars(cfg.Node, cfg.Design)

	if requiresProxy(cfg.Node) {
		// Proxy strategies deploy two workloads from the single canvas cache node:
		// 1) internal Redis (imageRedis)
		// 2) caching-proxy (HTTP->Redis->DB wrapper)
		redisLabels := map[string]string{"app": "redis-internal", "deployment": cfg.Namespace}
		internalRedisExporter := corev1.Container{
			Name:      "redis-exporter",
			Image:     "oliver006/redis_exporter:v1.67.0",
			Ports:     []corev1.ContainerPort{{ContainerPort: 9121}},
			Resources: ExporterResources(),
			Env:       []corev1.EnvVar{{Name: "REDIS_ADDR", Value: "localhost:6379"}},
		}
		internalRedisDep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "redis-internal", Namespace: cfg.Namespace},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr(replicas),
				Selector: &metav1.LabelSelector{MatchLabels: redisLabels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: redisLabels,
						Annotations: map[string]string{
							"prometheus.io/scrape": "true",
							"prometheus.io/port":   "9121",
							"prometheus.io/path":   "/metrics",
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:      "redis",
								Image:     imageRedis,
								Ports:     []corev1.ContainerPort{{ContainerPort: 6379}},
								Resources: resources,
								ReadinessProbe: &corev1.Probe{
									ProbeHandler: corev1.ProbeHandler{
										TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(6379)},
									},
									InitialDelaySeconds: 2,
									PeriodSeconds:       2,
									FailureThreshold:    3,
								},
							},
							internalRedisExporter,
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
		if err := k.waitForReady(ctx, cfg.Namespace, "redis-internal", 1); err != nil {
			return err
		}

		tmpl := templates.GetTemplate(cfg.Design.Template)
		if err := RequiresProxyQueries(cfg.Node, tmpl); err != nil {
			return err
		}
		queriesYAML, err := yaml.Marshal(tmpl.ProxyQueries)
		if err != nil {
			return fmt.Errorf("marshal proxyQueries: %w", err)
		}
		sum := sha256.Sum256(queriesYAML)
		checksum := hex.EncodeToString(sum[:])
		const proxyQueriesCM = "cache-proxy-queries"
		proxyQueriesConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: proxyQueriesCM, Namespace: cfg.Namespace},
			Data:       map[string]string{"queries.yaml": string(queriesYAML)},
		}
		if _, err := k.clientset.CoreV1().ConfigMaps(cfg.Namespace).Create(ctx, proxyQueriesConfigMap, metav1.CreateOptions{}); err != nil {
			if !errors.IsAlreadyExists(err) {
				return fmt.Errorf("create cache-proxy-queries configmap: %w", err)
			}
			if _, uerr := k.clientset.CoreV1().ConfigMaps(cfg.Namespace).Update(ctx, proxyQueriesConfigMap, metav1.UpdateOptions{}); uerr != nil {
				return fmt.Errorf("update cache-proxy-queries configmap: %w", uerr)
			}
		}

		proxyLabels := map[string]string{"app": "cache-proxy", "deployment": cfg.Namespace}
		proxyStrategy := cacheNodeStrategy(cfg.Node)
		proxyEnv := append(
			envMapToK8s(envVars),
			corev1.EnvVar{Name: "REDIS_HOST", Value: "redis-internal"},
			corev1.EnvVar{Name: "REDIS_PORT", Value: "6379"},
			corev1.EnvVar{Name: "PORT", Value: "8081"},
		)
		proxyContainer := newCacheProxyContainer(cacheImage(proxyStrategy), resources, proxyEnv)
		proxyContainer.VolumeMounts = []corev1.VolumeMount{{
			Name:      "proxy-queries",
			MountPath: "/etc/proxy/queries.yaml",
			SubPath:   "queries.yaml",
			ReadOnly:  true,
		}}

		proxyDep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "cache-proxy", Namespace: cfg.Namespace},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr(replicas),
				Selector: &metav1.LabelSelector{MatchLabels: proxyLabels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: proxyLabels,
						Annotations: map[string]string{
							"cloud9.io/proxy-queries-checksum": checksum,
							"prometheus.io/scrape":             "true",
							"prometheus.io/port":               "8081",
							"prometheus.io/path":               "/metrics",
						},
					},
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{
							Name: "proxy-queries",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: proxyQueriesCM},
								},
							},
						}},
						Containers: []corev1.Container{proxyContainer},
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

		return k.waitForReady(ctx, cfg.Namespace, "cache-proxy", replicas)
	}

	// Cache-aside: single Redis pod/service + redis_exporter sidecar for Prometheus.
	labels := map[string]string{"app": "redis", "deployment": cfg.Namespace}

	redisExporter := corev1.Container{
		Name:      "redis-exporter",
		Image:     "oliver006/redis_exporter:v1.67.0",
		Ports:     []corev1.ContainerPort{{ContainerPort: 9121}},
		Resources: ExporterResources(),
		Env:       []corev1.EnvVar{{Name: "REDIS_ADDR", Value: "localhost:6379"}},
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "9121",
						"prometheus.io/path":   "/metrics",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:      "redis",
							Image:     cacheImage("cache-aside"),
							Ports:     []corev1.ContainerPort{{ContainerPort: 6379}},
							Resources: resources,
							Env:       envMapToK8s(envVars),
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(6379)},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       2,
								FailureThreshold:    3,
							},
						},
						redisExporter,
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

	return k.waitForReady(ctx, cfg.Namespace, "redis", replicas)
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
						Name:            "app-server",
						Image:           appServerImage(cfg.Design.Template),
						ImagePullPolicy: corev1.PullAlways,
						Ports:           []corev1.ContainerPort{{ContainerPort: 8080}},
						Resources:       resources,
						// UNDERSTAND: readiness reflects only that the app can accept traffic
						// (/health/ready is dependency-free); liveness (/health/live) restarts
						// the container only when the process itself is wedged. This decouples
						// PodReady from transient Redis/DB slowness that used to drain the LB.
						ReadinessProbe: templateReadyProbe(8080, 3, 5, 8, 3),
						LivenessProbe:  templateLiveProbe(8080),
						Env:            envMapToK8s(envVars),
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

	return k.waitForReady(ctx, cfg.Namespace, "app-server", replicas)
}

func (k *K8sDeployer) deployWorker(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) error {
	envVars := DeriveEnvVars(cfg.Node, cfg.Design)
	labels := map[string]string{"app": "worker", "deployment": cfg.Namespace}
	workerAnnotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   "8080",
		"prometheus.io/path":   "/metrics",
	}
	img := workerImage(cfg.Design.Template)

	ctr := corev1.Container{
		Name:            "worker",
		Image:           img,
		ImagePullPolicy: corev1.PullAlways,
		Ports:           []corev1.ContainerPort{{ContainerPort: 8080}},
		Resources:       resources,
		Env:             envMapToK8s(envVars),
		ReadinessProbe:  templateHTTPReadinessProbe(8080, 5, 5, 10, 5),
	}
	// busybox exits immediately without a long-running command — keep pod Ready for QA placeholders.
	if img == imageBusybox || strings.HasSuffix(img, "/"+imageBusybox) {
		ctr.Command = []string{"sleep", "infinity"}
		ctr.Ports = nil
		ctr.ReadinessProbe = nil
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: workerAnnotations},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{ctr},
				},
			},
		},
	}
	_, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create worker deployment: %w", err)
	}
	return k.waitForReady(ctx, cfg.Namespace, "worker", replicas)
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

	// TCP readiness: PostgreSQL vs RabbitMQ use different startup timings.
	var probe *corev1.Probe
	tcpPort := intstr.FromInt32(spec.Ports[0])
	switch cfg.Node.Type {
	case "db":
		probe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: tcpPort},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       3,
			FailureThreshold:    5,
		}
	case "mq":
		probe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: tcpPort},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       3,
			FailureThreshold:    5,
		}
	default:
		probe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: tcpPort},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       3,
			FailureThreshold:    5,
		}
	}

	ports := make([]corev1.ContainerPort, len(spec.Ports))
	for i, p := range spec.Ports {
		ports[i] = corev1.ContainerPort{ContainerPort: p}
	}

	podAnnotations := make(map[string]string)
	if spec.Annotations != nil {
		for k, v := range spec.Annotations {
			podAnnotations[k] = v
		}
	}
	if spec.Exporter != nil {
		exp := spec.Exporter
		podAnnotations["prometheus.io/scrape"] = "true"
		podAnnotations["prometheus.io/port"] = strconv.Itoa(int(exp.Port))
		podAnnotations["prometheus.io/path"] = exp.MetricsPath
	}

	containers := []corev1.Container{{
		Name:           spec.AppLabel,
		Image:          spec.Image,
		Ports:          ports,
		Resources:      resources,
		Env:            envMapToK8s(allEnv),
		ReadinessProbe: probe,
	}}
	if len(spec.Command) > 0 {
		containers[0].Command = spec.Command
	}
	if spec.Exporter != nil {
		exp := spec.Exporter
		containers = append(containers, corev1.Container{
			Name:      exp.Name,
			Image:     exp.Image,
			Ports:     []corev1.ContainerPort{{ContainerPort: exp.Port}},
			Resources: ExporterResources(),
			Env:       envMapToK8s(exp.Env),
		})
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: spec.AppLabel, Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: podAnnotations},
				Spec: corev1.PodSpec{
					Containers: containers,
				},
			},
		},
	}

	_, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create deployment: %w", err)
	}

	if spec.NeedsService {
		// Expose all declared component ports on the Service (e.g. RabbitMQ AMQP + Prometheus).
		svcPorts := make([]corev1.ServicePort, 0, len(spec.Ports))
		for _, p := range spec.Ports {
			svcPorts = append(svcPorts, corev1.ServicePort{
				Name:       fmt.Sprintf("port-%d", p),
				Port:       p,
				TargetPort: intstr.FromInt32(p),
			})
		}
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: spec.ServiceName, Namespace: cfg.Namespace},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: labels,
				Ports:    svcPorts,
			},
		}
		_, err := k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svc, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create service: %w", err)
		}
	}

	return k.waitForReady(ctx, cfg.Namespace, spec.AppLabel, replicas)
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
	img := templates.GetAppServerImage(template)
	if img == "" {
		return imageNginxFull
	}
	return img
}

func workerImage(template string) string {
	img := templates.GetWorkerImage(template)
	if img == "" {
		return imageBusybox
	}
	return img
}

func cacheImage(strategy string) string {
	registry := os.Getenv("TEMPLATE_IMAGE_REGISTRY")
	if registry == "" {
		registry = "us-central1-docker.pkg.dev/runcloud9/cloud9-images"
	}
	tag := templates.ImageTag("caching-proxy")
	// UNDERSTAND: cache-aside uses plain Redis; proxy strategies use a custom Go service.
	// Canonicalize so legacy strategy=read-through still gets the proxy image.
	switch CanonicalCacheStrategy(strategy) {
	case "write-through", "write-behind":
		return registry + "/caching-proxy:" + tag
	default:
		return imageRedis
	}
}

func newCacheProxyContainer(image string, resources corev1.ResourceRequirements, env []corev1.EnvVar) corev1.Container {
	return corev1.Container{
		Name:            "cache-proxy",
		Image:           image,
		ImagePullPolicy: corev1.PullAlways,
		Ports:           []corev1.ContainerPort{{ContainerPort: 8081}},
		Resources:       resources,
		Env:             env,
		// Readiness pings only Redis (/health/ready); the DB check stays in /health so a
		// slow Postgres does not remove the proxy from rotation and cascade into the app tier.
		ReadinessProbe: templateReadyProbe(8081, 3, 5, 8, 5),
		LivenessProbe:  templateLiveProbe(8081),
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
