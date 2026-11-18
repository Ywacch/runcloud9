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

	"github.com/ywacch/cloud9/pkg/kubeobs"
	"github.com/ywacch/cloud9/pkg/templates"
	"gopkg.in/yaml.v3"
)

// Provisioning waits: fail fast on clearly unrecoverable container config, debounce CrashLoopBackOff
// so transient blips do not abort before Kubernetes has retried, but broken apps do not burn the
// full orchestration context (~8m on k8s). Image pull backoffs are left to the parent context.
const (
	waitReadyCrashLoopDebounce          = 45 * time.Second
	waitReadyRestartFailThreshold int32 = 3
	waitReadyPollInterval               = 500 * time.Millisecond

	// sessionImagePullPolicy uses the node-local image when the tag is already present.
	// Template tags in config/image-versions.json are immutable git SHAs; bump the tag to pull.
	sessionImagePullPolicy = corev1.PullIfNotPresent
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
	podLister PodLister
	endView   EndpointView
}

func NewK8sDeployer(clientset kubernetes.Interface, nodeType string) *K8sDeployer {
	return &K8sDeployer{clientset: clientset, nodeType: nodeType}
}

// SetEndpointView installs a cache-backed read path for LB upstream and weights.
// Nil restores API Get/Watch/List behavior.
func (k *K8sDeployer) SetEndpointView(view EndpointView) {
	if k == nil {
		return
	}
	k.endView = view
}

func (k *K8sDeployer) Deploy(ctx context.Context, cfg DeployConfig) error {
	ctx = kubeobs.WithCaller(ctx, "deployer")

	// namespace creation is idempotent-ish. each deployment gets a unique namespace from the timestamp
	nsLabels := map[string]string{}
	if v := namespaceExpiresAtLabelValue(cfg.ExpiresAt); v != "" {
		nsLabels[NamespaceExpiresAtLabel] = v
	}
	_, err := k.clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.Namespace, Labels: nsLabels},
	}, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace: %w", err)
	}
	if err := k.ensureNamespaceExpiresAtLabel(ctx, cfg.Namespace, cfg.ExpiresAt); err != nil {
		return fmt.Errorf("set namespace expiry label: %w", err)
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

	// replicas can be tuned per node type (app-server / worker bounds in ValidateRuntimeContract).
	replicas, err := deployReplicaCount(cfg.Node.Config, 1)
	if err != nil {
		return err
	}

	switch cfg.Node.Type {
	case "db":
		rr := dbReadReplicaCount(cfg)
		mode, err := parseReplicationMode(cfg.Node.Config)
		if err != nil {
			return err
		}
		total := 1 + int(rr)
		sizes := instanceSizesFromConfig(cfg.Node.Config, total)
		primaryResources := dbPrimaryResources(dbResourcesForSize(sizes[0]))
		if err := k.deployFromRegistry(ctx, cfg, primaryResources, replicas); err != nil {
			return err
		}
		minReady := seedReplicaReadyBeforeRestore(mode, rr)
		if minReady == 0 {
			invokeAfterPrimaryReady(ctx, cfg)
			if rr > 0 {
				return k.deployDBReadReplicas(ctx, cfg, rr)
			}
			return nil
		}
		desiredReady, err := k.createDBReadReplicaObjects(ctx, cfg, rr)
		if err != nil {
			return err
		}
		prefix := []PodWaitProgress{{Ordinal: 1, Ready: true, WaitCode: WaitCodeReady}}
		if err := k.waitForReady(ctx, cfg, dbReplicaAppLabel, minReady, waitProgressOpts{prefix: prefix}); err != nil {
			return err
		}
		if err := enablePostgresSynchronousReplication(ctx, k.newPostgresAdmin(cfg.Namespace), cfg.Node.Config, minReady); err != nil {
			return fmt.Errorf("enable postgres synchronous replication: %w", err)
		}
		invokeAfterPrimaryReady(ctx, cfg)
		if minReady < desiredReady {
			return k.waitForReady(ctx, cfg, dbReplicaAppLabel, desiredReady, waitProgressOpts{prefix: prefix})
		}
		return nil
	case "mq":
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

// waitForReady polls pod readiness until desiredReplicas are Ready or ctx ends.
// UNDERSTAND: a List-then-Watch without resourceVersion races when pods become Ready
// between the two calls; concurrent provisions hit that often. Re-listing each tick
// cannot miss a Ready transition that already happened.
// waitProgressOpts configures optional replica progress reporting during waitForReady.
type waitProgressOpts struct {
	prefix []PodWaitProgress
}

func (k *K8sDeployer) waitForReady(ctx context.Context, cfg DeployConfig, appLabel string, desiredReplicas int32, opts waitProgressOpts) error {
	if desiredReplicas <= 0 {
		desiredReplicas = 1
	}

	labelSelector := "deployment=" + cfg.Namespace + ",app=" + appLabel
	crashLoopSince := make(map[types.UID]time.Time)
	readyCount := 0
	var lastListed []corev1.Pod

	emitProgress := func() {
		if cfg.OnWaitProgress == nil {
			return
		}
		cfg.OnWaitProgress(cfg.Node.ID, buildPodWaitProgressList(lastListed, desiredReplicas, opts.prefix))
	}

	check := func() error {
		items, err := k.listPodsForReadiness(ctx, cfg.Namespace, labelSelector)
		if err != nil {
			return fmt.Errorf("list pods for %s: %w", appLabel, err)
		}
		readyPods := make(map[types.UID]struct{})
		seen := make(map[types.UID]struct{}, len(items))
		for i := range items {
			pod := &items[i]
			seen[pod.UID] = struct{}{}
			if pod.DeletionTimestamp != nil {
				continue
			}
			if err := detectWaitReadyFailure(pod, appLabel, crashLoopSince); err != nil {
				return fmt.Errorf("waiting for %s ready: %w", appLabel, err)
			}
			if podConditionReady(pod) {
				readyPods[pod.UID] = struct{}{}
			}
		}
		for uid := range crashLoopSince {
			if _, ok := seen[uid]; !ok {
				delete(crashLoopSince, uid)
			}
		}
		readyCount = len(readyPods)
		lastListed = items
		emitProgress()
		return nil
	}

	if err := check(); err != nil {
		return err
	}
	if int32(readyCount) >= desiredReplicas {
		return nil
	}

	ticker := time.NewTicker(waitReadyPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %d %s pod(s) ready (have %d): %w",
				desiredReplicas, appLabel, readyCount, ctx.Err())
		case <-ticker.C:
			if err := check(); err != nil {
				return err
			}
			if int32(readyCount) >= desiredReplicas {
				return nil
			}
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
				RevisionHistoryLimit: revisionHistoryLimitZero(),
				Replicas:             ptr(replicas),
				Selector:             &metav1.LabelSelector{MatchLabels: redisLabels},
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
								Args:      redisServerArgs(cfg.Node.Config, resources),
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
		applySessionPodPriority(&internalRedisDep.Spec.Template.Spec)
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
		if err := k.waitForReady(ctx, cfg, "redis-internal", 1, waitProgressOpts{}); err != nil {
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
		proxyResources := CacheProxyResources(cfg.Node.Config)
		proxyEnv := append(
			envMapToK8s(envVars),
			corev1.EnvVar{Name: "REDIS_HOST", Value: "redis-internal"},
			corev1.EnvVar{Name: "REDIS_PORT", Value: "6379"},
			corev1.EnvVar{Name: "PORT", Value: "8081"},
		)
		proxyContainer := newCacheProxyContainer(cacheImage(proxyStrategy), proxyResources, proxyEnv)
		proxyContainer.VolumeMounts = []corev1.VolumeMount{{
			Name:      "proxy-queries",
			MountPath: "/etc/proxy/queries.yaml",
			SubPath:   "queries.yaml",
			ReadOnly:  true,
		}}

		proxyDep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "cache-proxy", Namespace: cfg.Namespace},
			Spec: appsv1.DeploymentSpec{
				RevisionHistoryLimit: revisionHistoryLimitZero(),
				Replicas:             ptr(replicas),
				Selector:             &metav1.LabelSelector{MatchLabels: proxyLabels},
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
		applySessionPodPriority(&proxyDep.Spec.Template.Spec)
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

		return k.waitForReady(ctx, cfg, "cache-proxy", replicas, waitProgressOpts{})
	}

	engine := cacheEngineFromConfig(cfg.Node.Config)
	if engine == "memcached" {
		return k.deployMemcachedCacheAside(ctx, cfg, resources, replicas, envVars)
	}

	// Cache-aside: single Redis pod/service + redis_exporter sidecar for Prometheus.
	depName, svcName, appLabel := redisCacheDeployIdentity(cfg.Node, cfg.Design)
	labels := map[string]string{"app": appLabel, "deployment": cfg.Namespace}

	redisExporter := corev1.Container{
		Name:      "redis-exporter",
		Image:     "oliver006/redis_exporter:v1.67.0",
		Ports:     []corev1.ContainerPort{{ContainerPort: 9121}},
		Resources: ExporterResources(),
		Env:       []corev1.EnvVar{{Name: "REDIS_ADDR", Value: "localhost:6379"}},
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			RevisionHistoryLimit: revisionHistoryLimitZero(),
			Replicas:             ptr(replicas),
			Selector:             &metav1.LabelSelector{MatchLabels: labels},
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
							Args:      redisServerArgsForNode(cfg.Node, cfg.Design, resources),
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

	applySessionPodPriority(&dep.Spec.Template.Spec)
	_, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create deployment: %w", err)
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: cfg.Namespace},
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

	return k.waitForReady(ctx, cfg, appLabel, replicas, waitProgressOpts{})
}

func (k *K8sDeployer) deployMemcachedCacheAside(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32, envVars map[string]string) error {
	labels := map[string]string{"app": "memcached", "deployment": cfg.Namespace}

	exporter := corev1.Container{
		Name:      "memcached-exporter",
		Image:     "prom/memcached-exporter:v0.14.4",
		Ports:     []corev1.ContainerPort{{ContainerPort: 9150}},
		Resources: ExporterResources(),
		Env:       []corev1.EnvVar{{Name: "MEMCACHED_ADDR", Value: "localhost:11211"}},
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "memcached", Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			RevisionHistoryLimit: revisionHistoryLimitZero(),
			Replicas:             ptr(replicas),
			Selector:             &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "9150",
						"prometheus.io/path":   "/metrics",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:      "memcached",
							Image:     imageMemcached,
							Args:      memcachedServerArgs(cfg.Node.Config, resources),
							Ports:     []corev1.ContainerPort{{ContainerPort: 11211}},
							Resources: resources,
							Env:       envMapToK8s(envVars),
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(11211)},
								},
								InitialDelaySeconds: 2,
								PeriodSeconds:       2,
								FailureThreshold:    3,
							},
						},
						exporter,
					},
				},
			},
		},
	}

	applySessionPodPriority(&dep.Spec.Template.Spec)
	if _, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create memcached deployment: %w", err)
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: cfg.Namespace},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports:    []corev1.ServicePort{{Port: 11211, TargetPort: intstr.FromInt32(11211)}},
		},
	}
	if _, err := k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create memcached service: %w", err)
	}

	return k.waitForReady(ctx, cfg, "memcached", replicas, waitProgressOpts{})
}

func (k *K8sDeployer) deployAppServer(ctx context.Context, cfg DeployConfig, _ corev1.ResourceRequirements, replicas int32) error {
	envVars := DeriveEnvVars(cfg.Node, cfg.Design)
	sizes := instanceSizesFromConfig(cfg.Node.Config, int(replicas))
	groups := sizeGroups(sizes)

	svcSelector := map[string]string{"app": "app-server", "deployment": cfg.Namespace}
	appAnnotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   "8080",
		"prometheus.io/path":   "/metrics",
	}

	for i, group := range groups {
		depName := "app-server"
		if i > 0 {
			depName = "app-server-" + group.Size
		}
		labels := map[string]string{
			"app":                "app-server",
			"deployment":         cfg.Namespace,
			instanceSizeLabelKey: group.Size,
		}
		res := ResolveNodeResources("app-server", map[string]any{
			"instanceSize":    group.Size,
			"resourceProfile": normalizeInstanceSize(group.Size),
		})
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: cfg.Namespace},
			Spec: appsv1.DeploymentSpec{
				RevisionHistoryLimit: revisionHistoryLimitZero(),
				Replicas:             ptr(group.Count),
				Selector:             &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: appAnnotations},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:            "app-server",
							Image:           appServerImage(cfg.Design.Template),
							ImagePullPolicy: sessionImagePullPolicy,
							Ports:           []corev1.ContainerPort{{ContainerPort: 8080}},
							Resources:       res,
							ReadinessProbe:  templateReadyProbe(8080, 3, 5, 8, 3),
							LivenessProbe:   templateLiveProbe(8080),
							Env:             envMapToK8s(envVars),
						}},
					},
				},
			},
		}
		applySessionPodPriority(&dep.Spec.Template.Spec)
		if _, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create deployment %s: %w", depName, err)
		}
	}

	if ac, err := parseAutoscaleConfig(cfg.Node.Config); err != nil {
		return err
	} else if ac != nil {
		if len(groups) != 1 {
			return fmt.Errorf("autoscale requires uniform instance size across replicas")
		}
		if err := ensureCPUHorizontalPodAutoscaler(ctx, k.clientset, cfg.Namespace, "app-server", *ac); err != nil {
			return err
		}
	}

	svcApp := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: cfg.Namespace},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: svcSelector,
			Ports:    []corev1.ServicePort{{Port: 8080, TargetPort: intstr.FromInt32(8080)}},
		},
	}
	if _, err := k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svcApp, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	return k.waitForReady(ctx, cfg, "app-server", int32(len(sizes)), waitProgressOpts{})
}

func (k *K8sDeployer) deployWorker(ctx context.Context, cfg DeployConfig, _ corev1.ResourceRequirements, replicas int32) error {
	envVars := DeriveEnvVars(cfg.Node, cfg.Design)
	sizes := instanceSizesFromConfig(cfg.Node.Config, int(replicas))
	groups := sizeGroups(sizes)
	workerAnnotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   "8080",
		"prometheus.io/path":   "/metrics",
	}
	img := workerImage(cfg.Design.Template)

	for i, group := range groups {
		depName := "worker"
		if i > 0 {
			depName = "worker-" + group.Size
		}
		labels := map[string]string{
			"app":                "worker",
			"deployment":         cfg.Namespace,
			instanceSizeLabelKey: group.Size,
		}
		res := ResolveNodeResources("worker", map[string]any{
			"instanceSize":    group.Size,
			"resourceProfile": normalizeInstanceSize(group.Size),
		})

		ctr := corev1.Container{
			Name:            "worker",
			Image:           img,
			ImagePullPolicy: sessionImagePullPolicy,
			Ports:           []corev1.ContainerPort{{ContainerPort: 8080}},
			Resources:       res,
			Env:             envMapToK8s(envVars),
			ReadinessProbe:  templateHTTPReadinessProbe(8080, 5, 5, 10, 5),
		}
		if img == imageBusybox || strings.HasSuffix(img, "/"+imageBusybox) {
			ctr.Command = []string{"sleep", "infinity"}
			ctr.Ports = nil
			ctr.ReadinessProbe = nil
		}

		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: depName, Namespace: cfg.Namespace},
			Spec: appsv1.DeploymentSpec{
				RevisionHistoryLimit: revisionHistoryLimitZero(),
				Replicas:             ptr(group.Count),
				Selector:             &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: workerAnnotations},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{ctr},
					},
				},
			},
		}
		applySessionPodPriority(&dep.Spec.Template.Spec)
		if _, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create worker deployment %s: %w", depName, err)
		}
	}

	if ac, err := parseAutoscaleConfig(cfg.Node.Config); err != nil {
		return err
	} else if ac != nil {
		if len(groups) != 1 {
			return fmt.Errorf("autoscale requires uniform instance size across replicas")
		}
		if err := ensureCPUHorizontalPodAutoscaler(ctx, k.clientset, cfg.Namespace, "worker", *ac); err != nil {
			return err
		}
	}

	return k.waitForReady(ctx, cfg, "worker", int32(len(sizes)), waitProgressOpts{})
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
		if cfg.Node.Type == "db" {
			containers[0].Command = postgresPrimaryCommand(cfg.Node.Config)
		} else {
			containers[0].Command = spec.Command
		}
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

	var volumes []corev1.Volume
	if postgresReplicationHBAEnabled(cfg) {
		hbaCM := postgresReplicationHBAConfigMap(cfg.Namespace)
		if _, err := k.clientset.CoreV1().ConfigMaps(cfg.Namespace).Create(ctx, hbaCM, metav1.CreateOptions{}); err != nil {
			if !errors.IsAlreadyExists(err) {
				return fmt.Errorf("create postgres replication hba configmap: %w", err)
			}
		}
		containers[0].VolumeMounts = append(containers[0].VolumeMounts, postgresReplicationHBAVolumeMount())
		volumes = append(volumes, postgresReplicationHBAVolume())
	}
	if postgresExporterQueriesEnabled(cfg) && spec.Exporter != nil {
		queriesCM := postgresExporterQueriesConfigMap(cfg.Namespace)
		if _, err := k.clientset.CoreV1().ConfigMaps(cfg.Namespace).Create(ctx, queriesCM, metav1.CreateOptions{}); err != nil {
			if !errors.IsAlreadyExists(err) {
				return fmt.Errorf("create postgres-exporter-queries configmap: %w", err)
			}
		}
		exporterIdx := len(containers) - 1
		containers[exporterIdx].Args = postgresExporterExtendQueryArgs()
		containers[exporterIdx].VolumeMounts = append(containers[exporterIdx].VolumeMounts, postgresExporterQueriesVolumeMount())
		volumes = append(volumes, postgresExporterQueriesVolume())
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: spec.AppLabel, Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			RevisionHistoryLimit: revisionHistoryLimitZero(),
			Replicas:             ptr(replicas),
			Selector:             &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: podAnnotations},
				Spec: corev1.PodSpec{
					Volumes:    volumes,
					Containers: containers,
				},
			},
		},
	}

	applySessionPodPriority(&dep.Spec.Template.Spec)
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

	return k.waitForReady(ctx, cfg, spec.AppLabel, replicas, waitProgressOpts{})
}

func (k *K8sDeployer) Teardown(ctx context.Context, namespace string) error {
	ctx = kubeobs.WithCaller(ctx, "teardown")
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
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// revisionHistoryLimitZero is the history kept on every session Deployment and StatefulSet.
func revisionHistoryLimitZero() *int32 {
	return ptr(0)
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
		ImagePullPolicy: sessionImagePullPolicy,
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

func namespaceExpiresAtLabelValue(expiresAt time.Time) string {
	if expiresAt.IsZero() {
		return ""
	}
	return strconv.FormatInt(expiresAt.UTC().Unix(), 10)
}

func (k *K8sDeployer) ensureNamespaceExpiresAtLabel(ctx context.Context, namespace string, expiresAt time.Time) error {
	if k == nil || k.clientset == nil {
		if namespaceExpiresAtLabelValue(expiresAt) == "" {
			return nil
		}
		return fmt.Errorf("kubernetes client is required to set namespace expiry label")
	}
	return setNamespaceExpiresAtLabel(ctx, k.clientset, namespace, expiresAt)
}

// setNamespaceExpiresAtLabel writes cloud9.io/expires-at.
// A zero expiresAt is a no-op. A matching label returns without an update.
// Conflict retries re-read the namespace. GKE can mutate the object between get and update.
func setNamespaceExpiresAtLabel(ctx context.Context, client kubernetes.Interface, namespace string, expiresAt time.Time) error {
	val := namespaceExpiresAtLabelValue(expiresAt)
	if val == "" {
		return nil
	}
	if client == nil {
		return fmt.Errorf("kubernetes client is required to set namespace expiry label")
	}
	const attempts = 3
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("set namespace expiry label: %w", err)
		}
		ns, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get namespace: %w", err)
		}
		if ns.Labels != nil && ns.Labels[NamespaceExpiresAtLabel] == val {
			return nil
		}
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		ns.Labels[NamespaceExpiresAtLabel] = val
		if _, err := client.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{}); err != nil {
			if errors.IsConflict(err) {
				last = err
				continue
			}
			return fmt.Errorf("update namespace: %w", err)
		}
		return nil
	}
	return fmt.Errorf("update namespace: %w", last)
}

var _ ComponentDeployer = (*K8sDeployer)(nil)
