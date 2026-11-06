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

	"github.com/ywacch/cloud9/pkg/kubeobs"
)

const (
	lbConfigMapName              = "lb-config"
	lbNginxConfDir               = "/etc/nginx/cloud9"
	lbNginxConfPath              = "/etc/nginx/cloud9/nginx.conf"
	lbUpstreamFallbackAnnotation = "cloud9.io/lb-upstream-fallback"
	lbReconcileRetryWait         = 60 * time.Second
)

// lbNginxReloadScript starts nginx on the ConfigMap file and reloads when that
// file changes. A directory mount (not a subPath) receives ConfigMap updates.
// Rolling the Deployment to pick up a subPath file marks the pod unready, which
// the canvas paints as a red rim.
const lbNginxReloadScript = `CONF=` + lbNginxConfPath + `
nginx -c "$CONF" -g 'daemon off;' &
pid=$!
sum=$(md5sum "$CONF" 2>/dev/null | awk '{print $1}')
while kill -0 "$pid" 2>/dev/null; do
  sleep 2
  next=$(md5sum "$CONF" 2>/dev/null | awk '{print $1}')
  if [ -n "$next" ] && [ "$next" != "$sum" ]; then
    sum=$next
    nginx -s reload || true
  fi
done
wait "$pid"
`

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

// lbNginxContainer builds the demo LB nginx container. The command calls nginx
// directly so nginx:alpine does not run docker-entrypoint.d scripts that call apk.
// The ConfigMap is a directory mount. A loop reloads nginx when the file changes
// so an upstream edit does not roll the pod.
func lbNginxContainer(spec LBSpec) corev1.Container {
	return corev1.Container{
		Name:      "nginx",
		Image:     spec.Image,
		Command:   []string{"/bin/sh", "-c", lbNginxReloadScript},
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
			Name: "config", MountPath: lbNginxConfDir, ReadOnly: true,
		}},
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

// ReconcileLBUpstreamOptions holds optional read paths for reconcile.
type ReconcileLBUpstreamOptions struct {
	// EndpointView is non-nil when CLUSTERSTATE_LB uses the informer cache for upstream and weights.
	EndpointView EndpointView
}

// ReconcileLBUpstream re-resolves app-server pod IPs after the full deploy wave and
// updates lb-config so nginx balances per pod instead of a single service upstream.
func ReconcileLBUpstream(ctx context.Context, clientset kubernetes.Interface, namespace string, design DeploymentDesign, opts *ReconcileLBUpstreamOptions) error {
	ctx = kubeobs.WithCaller(ctx, "lb")
	lbNode, ok := FindLBNode(design)
	if !ok {
		return nil
	}

	k := NewK8sDeployer(clientset, "lb")
	if opts != nil && opts.EndpointView != nil {
		k.SetEndpointView(opts.EndpointView)
	}
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
	weighted := configBool(lbNode.Config, "weightByInstanceSize")
	var weights []int
	if weighted {
		weights = nginxWeightsForHosts(ctx, clientset, namespace, upstreamHosts, k.endView)
	}
	rate := rateLimitPerIP(lbNode.Config)
	nginxConf := spec.nginxConf(upstreamHosts, algo, weights, rate)

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
	return nil
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
	ctx = kubeobs.WithCaller(ctx, "lb")
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
	weighted := configBool(cfg.Node.Config, "weightByInstanceSize")
	var weights []int
	if weighted {
		weights = nginxWeightsForHosts(ctx, k.clientset, cfg.Namespace, upstreamHosts, k.endView)
	}
	rate := rateLimitPerIP(cfg.Node.Config)
	nginxConf := spec.nginxConf(upstreamHosts, algo, weights, rate)

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
					Containers: []corev1.Container{lbNginxContainer(spec)},
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

	return k.waitForReady(ctx, cfg, "lb", replicas, waitProgressOpts{})
}

// nginxConfig builds a minimal nginx.conf with an upstream and reverse proxy to app backends.
// algorithm: round-robin (default), least_conn, random — directives other than the default
// must appear inside upstream {}.
func (s LBSpec) nginxConfig(backends []string, algorithm string) string {
	return s.nginxConfigWeighted(backends, algorithm, nil)
}

func (s LBSpec) nginxConfigWeighted(backends []string, algorithm string, weights []int) string {
	return s.nginxConf(backends, algorithm, weights, 0)
}

func (s LBSpec) nginxConf(backends []string, algorithm string, weights []int, rateLimit int) string {
	var upstreamExtra string
	switch normalizeLBAlgorithm(algorithm) {
	case lbAlgoLeastConn:
		upstreamExtra = "        least_conn;\n"
	case lbAlgoRandom:
		upstreamExtra = "        random;\n"
	case lbAlgoIPHash:
		upstreamExtra = "        ip_hash;\n"
	case lbAlgoLeastTime:
		upstreamExtra = "        least_time header;\n"
	default:
		upstreamExtra = ""
	}
	useWeights := len(weights) == len(backends) && len(weights) > 0 && !weightsAllEqual(weights)
	for _, w := range weights {
		if w <= 0 {
			useWeights = false
			break
		}
	}
	var b strings.Builder
	for i, host := range backends {
		if useWeights {
			fmt.Fprintf(&b, "        server %s:8080 weight=%d;\n", host, weights[i])
		} else {
			fmt.Fprintf(&b, "        server %s:8080;\n", host)
		}
	}
	limitZone := ""
	limitLoc := ""
	if rateLimit > 0 {
		limitZone = fmt.Sprintf(
			"    limit_req_zone $binary_remote_addr zone=perip:1m rate=%dr/s;\n    limit_req_status 429;\n",
			rateLimit,
		)
		// Omit burst= (nginx rejects explicit burst=0). Default burst is 0: excess requests return 429 via limit_req_status.
		limitLoc = "limit_req zone=perip nodelay;\n            "
	}
	return fmt.Sprintf(`events {
    worker_connections %d;
}
http {
    set_real_ip_from 0.0.0.0/0;
    real_ip_header X-Forwarded-For;
    real_ip_recursive on;
%s    upstream backends {
%s%s        keepalive %d;
    }

    server {
        listen 80;
        location /nginx_status {
            stub_status;
            allow all;
        }
        location / {
            %sproxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_connect_timeout 5s;
            proxy_send_timeout 60s;
            proxy_read_timeout 60s;
            proxy_pass http://backends;
        }
    }
}
`, s.WorkerConnections, limitZone, upstreamExtra, b.String(), s.UpstreamKeepalive, limitLoc)
}

// nginxWeightsForHosts maps upstream pod IPs to nginx weights from instance-size labels.
func nginxWeightsForHosts(ctx context.Context, clientset kubernetes.Interface, namespace string, upstreamHosts []string, view EndpointView) []int {
	if len(upstreamHosts) == 0 {
		return nil
	}
	var items []corev1.Pod
	if view != nil {
		ptrs, err := view.Pods(namespace)
		if err != nil {
			return nil
		}
		sel := fmt.Sprintf("app=app-server,deployment=%s", namespace)
		filtered, err := podsMatchingEqualSelector(ptrs, sel)
		if err != nil {
			return nil
		}
		items = filtered
	} else {
		list, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=app-server,deployment=%s", namespace),
		})
		if err != nil {
			return nil
		}
		items = list.Items
	}
	ipToWeight := make(map[string]int, len(items))
	for _, p := range items {
		if p.Status.PodIP == "" {
			continue
		}
		size := p.Labels[instanceSizeLabelKey]
		if size == "" {
			return nil
		}
		ipToWeight[p.Status.PodIP] = nginxWeightForSize(size)
	}
	weights := make([]int, len(upstreamHosts))
	for i, host := range upstreamHosts {
		w, ok := ipToWeight[host]
		if !ok {
			return nil
		}
		weights[i] = w
	}
	return weights
}

// resolveAppServerUpstream returns nginx upstream hosts (pod IPs when available, else service name).
// Requires cloud9-api ClusterRole get/list on core Endpoints; without it Get fails and callers
// receive the app-server service fallback.
func (k *K8sDeployer) resolveAppServerUpstream(ctx context.Context, namespace string) (hosts []string, endpoints []LBEndpointSnapshot) {
	if k.endView != nil {
		return resolveAppServerUpstreamFromView(k.endView, namespace)
	}
	ep, err := k.clientset.CoreV1().Endpoints(namespace).Get(ctx, "app-server", metav1.GetOptions{})
	if err != nil || len(ep.Subsets) == 0 {
		return []string{"app-server"}, nil
	}
	h, snap := hostsFromEndpoints(ep)
	if len(h) == 0 {
		return []string{"app-server"}, nil
	}
	return h, snap
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
