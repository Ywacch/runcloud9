package deployer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	lbConfigMapName              = "lb-config"
	lbConfigReloadAnnotation     = "cloud9.io/lb-config-reload"
	lbUpstreamFallbackAnnotation = "cloud9.io/lb-upstream-fallback"
	lbReconcileRetryWait         = 60 * time.Second
)

// LBSpec describes the demo nginx load balancer (explicit deploy path, not componentRegistry).
type LBSpec struct {
	AppLabel           string
	Image              string
	Port               int32
	Resources          corev1.ResourceRequirements
	WorkerConnections  int
	UpstreamKeepalive  int
	UpstreamRetryWait  time.Duration
	UpstreamRetryEvery time.Duration
}

// DefaultLBSpec returns demo nginx sizing tuned for ~200 RPS through the LB.
func DefaultLBSpec() LBSpec {
	return LBSpec{
		AppLabel:           "lb",
		Image:              imageNginx,
		Port:               80,
		Resources:          ComponentDefaultResources("lb"),
		WorkerConnections:  4096,
		UpstreamKeepalive:  256,
		UpstreamRetryWait:  30 * time.Second,
		UpstreamRetryEvery: 2 * time.Second,
	}
}

// LBEndpointSnapshot records a ready app-server pod IP at deploy time for metrics and debugging.
type LBEndpointSnapshot struct {
	PodName string `json:"podName"`
	PodIP   string `json:"podIP"`
	Ready   bool   `json:"ready"`
}

// FindLBNode returns the first lb node in the design, if any.
func FindLBNode(design DeploymentDesign) (DeploymentNode, bool) {
	for _, n := range design.Nodes {
		if n.Type == "lb" {
			return n, true
		}
	}
	return DeploymentNode{}, false
}

// ReconcileLBUpstream re-resolves app-server pod IPs after the full deploy wave and
// updates lb-config so nginx balances per pod instead of a single service upstream.
func ReconcileLBUpstream(ctx context.Context, clientset kubernetes.Interface, namespace string, design DeploymentDesign) error {
	lbNode, ok := FindLBNode(design)
	if !ok {
		return nil
	}

	k := NewK8sDeployer(clientset, "lb")
	spec := DefaultLBSpec()
	appReplicas := appServerDesiredReplicas(lbNode, design)
	upstreamHosts, endpointSnap := waitResolveAppServerUpstream(
		ctx, k, namespace, appReplicas, lbReconcileRetryWait, spec.UpstreamRetryEvery,
	)
	if isUpstreamServiceFallback(upstreamHosts, endpointSnap) {
		if _, err := clientset.CoreV1().Endpoints(namespace).Get(ctx, "app-server", metav1.GetOptions{}); err != nil {
			return fmt.Errorf("app-server endpoints unavailable (cloud9-api needs get/list on endpoints): %w", err)
		}
		return fmt.Errorf("app-server endpoints unavailable: nginx would use service fallback")
	}

	algo := normalizeLBAlgorithm(configString(lbNode.Config, "lbAlgorithm", lbAlgoRoundRobin))
	if cm, err := clientset.CoreV1().ConfigMaps(namespace).Get(ctx, lbConfigMapName, metav1.GetOptions{}); err == nil {
		if v := cm.Annotations["cloud9.io/lb-algorithm"]; v != "" {
			algo = normalizeLBAlgorithm(v)
		}
	}

	logicalBackends := lbBackends(lbNode, design)
	nginxConf := spec.nginxConfig(upstreamHosts, algo)

	cm, err := buildLBConfigMap(lbConfigInput{
		algo:            algo,
		logicalBackends: logicalBackends,
		appReplicas:     appReplicas,
		upstreamHosts:   upstreamHosts,
		endpointSnap:    endpointSnap,
		nginxConf:       nginxConf,
		serviceFallback: false,
	})
	if err != nil {
		return err
	}
	cm.Namespace = namespace

	existing, err := clientset.CoreV1().ConfigMaps(namespace).Get(ctx, lbConfigMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get lb configmap: %w", err)
	}
	if existing.Data["nginx.conf"] == nginxConf {
		return nil
	}

	cm.ObjectMeta.ResourceVersion = existing.ResourceVersion
	if _, err := clientset.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update lb configmap: %w", err)
	}
	return k.reloadLBDeployment(ctx, namespace)
}

type lbConfigInput struct {
	algo            string
	logicalBackends []string
	appReplicas     int32
	upstreamHosts   []string
	endpointSnap    []LBEndpointSnapshot
	nginxConf       string
	serviceFallback bool
}

func buildLBConfigMap(in lbConfigInput) (*corev1.ConfigMap, error) {
	endpointsJSON, err := json.Marshal(in.endpointSnap)
	if err != nil {
		return nil, fmt.Errorf("marshal lb endpoints: %w", err)
	}
	fallback := "false"
	if in.serviceFallback {
		fallback = "true"
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: lbConfigMapName,
			Annotations: map[string]string{
				"cloud9.io/lb-algorithm":     in.algo,
				"cloud9.io/lb-backends":      strings.Join(in.logicalBackends, ","),
				"cloud9.io/lb-endpoints":     string(endpointsJSON),
				"cloud9.io/lb-app-replicas":  strconv.FormatInt(int64(in.appReplicas), 10),
				lbUpstreamFallbackAnnotation: fallback,
			},
		},
		Data: map[string]string{"nginx.conf": in.nginxConf},
	}, nil
}

func isUpstreamServiceFallback(hosts []string, endpoints []LBEndpointSnapshot) bool {
	return len(endpoints) == 0 && len(hosts) == 1 && hosts[0] == "app-server"
}

func (k *K8sDeployer) deployLB(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) error {
	spec := DefaultLBSpec()
	spec.Resources = resources
	algo := normalizeLBAlgorithm(configString(cfg.Node.Config, "lbAlgorithm", lbAlgoRoundRobin))
	logicalBackends := lbBackends(cfg.Node, cfg.Design)
	if len(logicalBackends) == 0 {
		return fmt.Errorf("lb has no app-server backends: add edges from lb to app-server")
	}
	appReplicas := appServerDesiredReplicas(cfg.Node, cfg.Design)
	upstreamHosts, endpointSnap := k.waitResolveAppServerUpstream(ctx, cfg.Namespace, appReplicas, spec)
	serviceFallback := isUpstreamServiceFallback(upstreamHosts, endpointSnap)
	nginxConf := spec.nginxConfig(upstreamHosts, algo)

	cm, err := buildLBConfigMap(lbConfigInput{
		algo:            algo,
		logicalBackends: logicalBackends,
		appReplicas:     appReplicas,
		upstreamHosts:   upstreamHosts,
		endpointSnap:    endpointSnap,
		nginxConf:       nginxConf,
		serviceFallback: serviceFallback,
	})
	if err != nil {
		return err
	}
	cm.Namespace = cfg.Namespace

	if _, err = k.clientset.CoreV1().ConfigMaps(cfg.Namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create lb configmap: %w", err)
	}

	labels := map[string]string{"app": "lb", "deployment": cfg.Namespace}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "lb", Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:      "nginx",
						Image:     spec.Image,
						Ports:     []corev1.ContainerPort{{ContainerPort: spec.Port}},
						Resources: spec.Resources,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(spec.Port)},
							},
							InitialDelaySeconds: 2,
							PeriodSeconds:       2,
							FailureThreshold:    3,
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "config", MountPath: "/etc/nginx/nginx.conf", SubPath: "nginx.conf",
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: lbConfigMapName},
							},
						},
					}},
				},
			},
		},
	}
	if _, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create lb deployment: %w", err)
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "lb", Namespace: cfg.Namespace},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports:    []corev1.ServicePort{{Port: spec.Port, TargetPort: intstr.FromInt32(spec.Port)}},
		},
	}
	if _, err := k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create lb service: %w", err)
	}

	return k.waitForReady(ctx, cfg.Namespace, "lb", replicas)
}

func (k *K8sDeployer) reloadLBDeployment(ctx context.Context, namespace string) error {
	dep, err := k.clientset.AppsV1().Deployments(namespace).Get(ctx, "lb", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get lb deployment: %w", err)
	}
	if dep.Spec.Template.Annotations == nil {
		dep.Spec.Template.Annotations = map[string]string{}
	}
	dep.Spec.Template.Annotations[lbConfigReloadAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := k.clientset.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update lb deployment: %w", err)
	}
	replicas := int32(1)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}
	return k.waitForReady(ctx, namespace, "lb", replicas)
}

// nginxConfig builds a minimal nginx.conf with an upstream and reverse proxy to app backends.
// algorithm: round-robin (default), least_conn, random — directives other than the default
// must appear inside upstream {}.
func (s LBSpec) nginxConfig(backends []string, algorithm string) string {
	var upstreamExtra string
	switch normalizeLBAlgorithm(algorithm) {
	case lbAlgoLeastConn:
		upstreamExtra = "        least_conn;\n"
	case lbAlgoRandom:
		upstreamExtra = "        random;\n"
	default:
		upstreamExtra = ""
	}
	var b strings.Builder
	for _, host := range backends {
		fmt.Fprintf(&b, "        server %s:8080;\n", host)
	}
	return fmt.Sprintf(`events {
    worker_connections %d;
}
http {
    upstream backends {
%s%s        keepalive %d;
    }

    server {
        listen 80;
        location /nginx_status {
            stub_status;
            allow all;
        }
        location / {
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_connect_timeout 5s;
            proxy_send_timeout 60s;
            proxy_read_timeout 60s;
            proxy_pass http://backends;
        }
    }
}
`, s.WorkerConnections, upstreamExtra, b.String(), s.UpstreamKeepalive)
}

// resolveAppServerUpstream returns nginx upstream hosts (pod IPs when available, else service name).
// Requires cloud9-api ClusterRole get/list on core Endpoints; without it Get fails and callers
// receive the app-server service fallback.
func (k *K8sDeployer) resolveAppServerUpstream(ctx context.Context, namespace string) (hosts []string, endpoints []LBEndpointSnapshot) {
	ep, err := k.clientset.CoreV1().Endpoints(namespace).Get(ctx, "app-server", metav1.GetOptions{})
	if err != nil || len(ep.Subsets) == 0 {
		return []string{"app-server"}, nil
	}
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			if addr.IP == "" {
				continue
			}
			hosts = append(hosts, addr.IP)
			name := ""
			if addr.TargetRef != nil {
				name = addr.TargetRef.Name
			}
			endpoints = append(endpoints, LBEndpointSnapshot{
				PodName: name,
				PodIP:   addr.IP,
				Ready:   true,
			})
		}
	}
	if len(hosts) == 0 {
		return []string{"app-server"}, nil
	}
	return hosts, endpoints
}

// waitResolveAppServerUpstream retries until app-server Endpoints publish ready pod IPs.
// Endpoints can lag pod Ready briefly after waitForReady returns; without retry nginx gets
// a service-name fallback and lb-endpoints stays null. Returns the best snapshot found.
func (k *K8sDeployer) waitResolveAppServerUpstream(ctx context.Context, namespace string, desired int32, spec LBSpec) ([]string, []LBEndpointSnapshot) {
	return waitResolveAppServerUpstream(ctx, k, namespace, desired, spec.UpstreamRetryWait, spec.UpstreamRetryEvery)
}

func waitResolveAppServerUpstream(
	ctx context.Context,
	k *K8sDeployer,
	namespace string,
	desired int32,
	maxWait time.Duration,
	interval time.Duration,
) ([]string, []LBEndpointSnapshot) {
	if desired < 1 {
		desired = 1
	}
	deadline := time.Now().Add(maxWait)
	var bestHosts []string
	var bestEndpoints []LBEndpointSnapshot

	for {
		hosts, endpoints := k.resolveAppServerUpstream(ctx, namespace)
		if len(endpoints) > 0 {
			bestHosts = hosts
			bestEndpoints = endpoints
			if int32(len(endpoints)) >= desired {
				return bestHosts, bestEndpoints
			}
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			if len(bestEndpoints) > 0 {
				return bestHosts, bestEndpoints
			}
			hosts, endpoints := k.resolveAppServerUpstream(ctx, namespace)
			if len(endpoints) > 0 {
				return hosts, endpoints
			}
			return []string{"app-server"}, nil
		case <-time.After(interval):
		}
	}
	if len(bestEndpoints) > 0 {
		return bestHosts, bestEndpoints
	}
	return []string{"app-server"}, nil
}

// appServerDesiredReplicas sums replicas from app-server nodes connected to the LB.
func appServerDesiredReplicas(lbNode DeploymentNode, design DeploymentDesign) int32 {
	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))
	for _, n := range design.Nodes {
		nodeMap[n.ID] = n
	}
	var total int32
	for _, c := range design.Connections {
		if c.SourceID != lbNode.ID {
			continue
		}
		target, ok := nodeMap[c.TargetID]
		if !ok || target.Type != "app-server" {
			continue
		}
		total += parseReplicaCount(target.Config, 1)
	}
	if total == 0 {
		return 1
	}
	return total
}

// lbBackends extracts K8s service hostnames for app-servers reached by outbound edges from the lb node.
// Today every app-server uses the shared Service name "app-server".
func lbBackends(lbNode DeploymentNode, design DeploymentDesign) []string {
	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))
	for _, n := range design.Nodes {
		nodeMap[n.ID] = n
	}
	seen := make(map[string]struct{})
	var backends []string
	for _, c := range design.Connections {
		if c.SourceID != lbNode.ID {
			continue
		}
		t, ok := nodeMap[c.TargetID]
		if !ok || t.Type != "app-server" {
			continue
		}
		host := "app-server"
		if _, dup := seen[host]; dup {
			continue
		}
		seen[host] = struct{}{}
		backends = append(backends, host)
	}
	return backends
}
