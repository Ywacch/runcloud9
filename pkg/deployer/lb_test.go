package deployer

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestLbNginxContainer_skipsDockerEntrypoint(t *testing.T) {
	t.Parallel()
	c := lbNginxContainer(DefaultLBSpec())
	if len(c.Command) != 1 || c.Command[0] != "nginx" {
		t.Fatalf("Command = %v want [nginx]", c.Command)
	}
	if len(c.Args) != 2 || c.Args[0] != "-g" || c.Args[1] != "daemon off;" {
		t.Fatalf("Args = %v want [-g daemon off;]", c.Args)
	}
	if c.Image != imageNginx {
		t.Fatalf("Image = %q want %q", c.Image, imageNginx)
	}
}

func TestDefaultLBSpec(t *testing.T) {
	t.Parallel()
	spec := DefaultLBSpec()
	if spec.Image != imageNginx {
		t.Fatalf("Image = %q want %q", spec.Image, imageNginx)
	}
	if spec.WorkerConnections != 4096 || spec.UpstreamKeepalive != 256 {
		t.Fatalf("nginx tuning: worker_connections=%d keepalive=%d", spec.WorkerConnections, spec.UpstreamKeepalive)
	}
	cpu := spec.Resources.Limits.Cpu().String()
	if cpu != "250m" {
		t.Fatalf("CPU limit = %q want 250m", cpu)
	}
}

func TestLbBackends(t *testing.T) {
	t.Parallel()
	lb := DeploymentNode{ID: "lb1", Type: "lb", Name: "LB"}
	app := DeploymentNode{ID: "a1", Type: "app-server", Name: "App"}
	other := DeploymentNode{ID: "db1", Type: "db", Name: "DB"}
	design := DeploymentDesign{
		Nodes: []DeploymentNode{lb, app, other},
		Connections: []DeploymentConn{
			{SourceID: "lb1", TargetID: "a1"},
			{SourceID: "lb1", TargetID: "db1"},
		},
	}
	got := lbBackends(lb, design)
	if len(got) != 1 || got[0] != "app-server" {
		t.Fatalf("lbBackends: got %v want [app-server]", got)
	}
}

func TestLBSpec_nginxConfig(t *testing.T) {
	t.Parallel()
	spec := DefaultLBSpec()
	backends := []string{"app-server"}

	rr := spec.nginxConfig(backends, "round-robin")
	if !strings.Contains(rr, "upstream backends") || !strings.Contains(rr, "proxy_pass http://backends") {
		t.Fatalf("round-robin config: %s", rr)
	}
	if !strings.Contains(rr, "real_ip_header") || !strings.Contains(rr, "set_real_ip_from") {
		t.Fatalf("round-robin config missing real_ip: %s", rr)
	}
	if !strings.Contains(rr, "location /nginx_status") || !strings.Contains(rr, "stub_status") {
		t.Fatalf("round-robin config missing stub_status: %s", rr)
	}
	if strings.Contains(rr, "least_conn") || strings.Contains(rr, "random") || strings.Contains(rr, "ip_hash") || strings.Contains(rr, "least_time") {
		t.Fatalf("unexpected alg directives in round-robin: %s", rr)
	}

	lc := spec.nginxConfig(backends, "least_conn")
	if !strings.Contains(lc, "least_conn") {
		t.Fatalf("least_conn: %s", lc)
	}

	rnd := spec.nginxConfig(backends, "random")
	if !strings.Contains(rnd, "random;") {
		t.Fatalf("random: %s", rnd)
	}

	ipHash := spec.nginxConfig(backends, "ip_hash")
	if !strings.Contains(ipHash, "ip_hash;") {
		t.Fatalf("ip_hash should emit sticky directive: %s", ipHash)
	}

	leastTime := spec.nginxConfig(backends, "least_time")
	if !strings.Contains(leastTime, "least_time header;") {
		t.Fatalf("least_time: %s", leastTime)
	}

	multi := spec.nginxConfig([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, "least_conn")
	if !strings.Contains(multi, "server 10.0.0.1:8080") || !strings.Contains(multi, "server 10.0.0.3:8080") {
		t.Fatalf("multi upstream: %s", multi)
	}
	if !strings.Contains(multi, "worker_connections 4096") {
		t.Fatalf("expected worker_connections 4096: %s", multi)
	}
	if !strings.Contains(multi, "keepalive 256") {
		t.Fatalf("expected upstream keepalive: %s", multi)
	}
	if !strings.Contains(multi, "proxy_http_version 1.1") || !strings.Contains(multi, "proxy_read_timeout 60s") {
		t.Fatalf("expected proxy tuning: %s", multi)
	}
}

func TestLBSpec_nginxConfigWeighted(t *testing.T) {
	t.Parallel()
	spec := DefaultLBSpec()
	backends := []string{"10.0.0.1", "10.0.0.2"}

	off := spec.nginxConfigWeighted(backends, "round-robin", nil)
	if strings.Contains(off, "weight=") {
		t.Fatalf("nil weights should omit weight=: %s", off)
	}

	uniform := spec.nginxConfigWeighted(backends, "round-robin", []int{1, 1})
	if strings.Contains(uniform, "weight=") {
		t.Fatalf("uniform weights should omit weight=: %s", uniform)
	}

	mixed := spec.nginxConfigWeighted(backends, "round-robin", []int{4, 1})
	if !strings.Contains(mixed, "server 10.0.0.1:8080 weight=4;") {
		t.Fatalf("expected weight=4 on large backend: %s", mixed)
	}
	if !strings.Contains(mixed, "server 10.0.0.2:8080 weight=1;") {
		t.Fatalf("expected weight=1 on small backend: %s", mixed)
	}
}

func TestLBSpec_nginxConf_rateLimit(t *testing.T) {
	t.Parallel()
	spec := DefaultLBSpec()
	backends := []string{"10.0.0.1"}

	off := spec.nginxConf(backends, "round-robin", nil, 0)
	if strings.Contains(off, "limit_req") || strings.Contains(off, "rate=") || strings.Contains(off, "limit_req_status") {
		t.Fatalf("rate=0 should omit limit_req: %s", off)
	}

	limited := spec.nginxConf(backends, "round-robin", nil, 5)
	if !strings.Contains(limited, "rate=5r/s") {
		t.Fatalf("expected rate=5r/s: %s", limited)
	}
	if !strings.Contains(limited, "limit_req_status 429") {
		t.Fatalf("expected limit_req_status 429: %s", limited)
	}
	if !strings.Contains(limited, "limit_req zone=perip nodelay") {
		t.Fatalf("expected limit_req zone=perip nodelay: %s", limited)
	}
	if strings.Contains(limited, "burst=") {
		t.Fatalf("explicit burst= is invalid at 0 and not needed: %s", limited)
	}
	parts := strings.Split(limited, "location / {")
	if len(parts) != 2 {
		t.Fatalf("expected location / block: %s", limited)
	}
	if strings.Contains(parts[0], "limit_req zone=perip") {
		t.Fatalf("limit_req must not apply to /nginx_status: %s", limited)
	}
}

func TestAppServerDesiredReplicas(t *testing.T) {
	t.Parallel()
	lb := DeploymentNode{ID: "lb1", Type: "lb", Name: "LB"}
	app := DeploymentNode{ID: "a1", Type: "app-server", Name: "App", Config: map[string]interface{}{"replicas": 3}}
	design := DeploymentDesign{
		Nodes:       []DeploymentNode{lb, app},
		Connections: []DeploymentConn{{SourceID: "lb1", TargetID: "a1"}},
	}
	if got := appServerDesiredReplicas(lb, design); got != 3 {
		t.Fatalf("got %d want 3", got)
	}
}

func TestResolveAppServerUpstream(t *testing.T) {
	t.Parallel()

	ns := "cloud9-upstream-test"
	tests := []struct {
		name      string
		endpoints *corev1.Endpoints
		wantHosts []string
		wantLen   int
	}{
		{
			name:      "missing endpoints falls back to service",
			endpoints: nil,
			wantHosts: []string{"app-server"},
			wantLen:   0,
		},
		{
			name: "ready pod IPs",
			endpoints: &corev1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
				Subsets: []corev1.EndpointSubset{{
					Addresses: []corev1.EndpointAddress{
						{IP: "10.0.0.1", TargetRef: &corev1.ObjectReference{Name: "app-server-a"}},
						{IP: "10.0.0.2", TargetRef: &corev1.ObjectReference{Name: "app-server-b"}},
					},
				}},
			},
			wantHosts: []string{"10.0.0.1", "10.0.0.2"},
			wantLen:   2,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var client *fake.Clientset
			if tc.endpoints != nil {
				client = fake.NewSimpleClientset(tc.endpoints)
			} else {
				client = fake.NewSimpleClientset()
			}
			d := NewK8sDeployer(client, "lb")
			hosts, snaps := d.resolveAppServerUpstream(context.Background(), ns)
			if len(snaps) != tc.wantLen {
				t.Fatalf("snapshots len = %d want %d", len(snaps), tc.wantLen)
			}
			if len(hosts) != len(tc.wantHosts) {
				t.Fatalf("hosts = %v want %v", hosts, tc.wantHosts)
			}
			for i := range tc.wantHosts {
				if hosts[i] != tc.wantHosts[i] {
					t.Fatalf("hosts[%d] = %q want %q", i, hosts[i], tc.wantHosts[i])
				}
			}
		})
	}
}

func TestWaitResolveAppServerUpstream(t *testing.T) {
	t.Parallel()

	ns := "cloud9-wait-upstream"
	ep := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{
				{IP: "10.0.0.1", TargetRef: &corev1.ObjectReference{Name: "app-server-0"}},
				{IP: "10.0.0.2", TargetRef: &corev1.ObjectReference{Name: "app-server-1"}},
				{IP: "10.0.0.3", TargetRef: &corev1.ObjectReference{Name: "app-server-2"}},
			},
		}},
	}
	client := fake.NewSimpleClientset(ep)
	d := NewK8sDeployer(client, "lb")

	hosts, snaps := waitResolveAppServerUpstream(
		context.Background(), d, ns, 3, 50*time.Millisecond, 5*time.Millisecond,
	)
	if len(snaps) != 3 {
		t.Fatalf("snapshots len = %d want 3", len(snaps))
	}
	if len(hosts) != 3 || hosts[0] != "10.0.0.1" {
		t.Fatalf("hosts = %v", hosts)
	}
}

func TestWaitResolveAppServerUpstream_eventuallyPopulated(t *testing.T) {
	ns := "cloud9-wait-eventual"
	client := fake.NewSimpleClientset()
	d := NewK8sDeployer(client, "lb")

	go func() {
		time.Sleep(20 * time.Millisecond)
		_, _ = client.CoreV1().Endpoints(ns).Create(context.Background(), &corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
			Subsets: []corev1.EndpointSubset{{
				Addresses: []corev1.EndpointAddress{
					{IP: "10.0.0.9", TargetRef: &corev1.ObjectReference{Name: "app-server-0"}},
				},
			}},
		}, metav1.CreateOptions{})
	}()

	hosts, snaps := waitResolveAppServerUpstream(
		context.Background(), d, ns, 1, 200*time.Millisecond, 10*time.Millisecond,
	)
	if len(snaps) != 1 || snaps[0].PodIP != "10.0.0.9" {
		t.Fatalf("snapshots = %+v", snaps)
	}
	if len(hosts) != 1 || hosts[0] != "10.0.0.9" {
		t.Fatalf("hosts = %v", hosts)
	}
}

func TestWaitResolveAppServerUpstream_timeoutFallback(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	d := NewK8sDeployer(client, "lb")
	hosts, snaps := waitResolveAppServerUpstream(
		context.Background(), d, "cloud9-empty", 3, 20*time.Millisecond, 5*time.Millisecond,
	)
	if len(snaps) != 0 {
		t.Fatalf("snapshots = %+v want none", snaps)
	}
	if len(hosts) != 1 || hosts[0] != "app-server" {
		t.Fatalf("hosts = %v want [app-server]", hosts)
	}
}

func TestFindLBNode(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "lb1", Type: "lb", Name: "LB"},
			{ID: "a1", Type: "app-server", Name: "App"},
		},
	}
	node, ok := FindLBNode(design)
	if !ok || node.ID != "lb1" {
		t.Fatalf("FindLBNode: got %+v ok=%v", node, ok)
	}
}

func TestIsUpstreamServiceFallback(t *testing.T) {
	t.Parallel()
	if !isUpstreamServiceFallback([]string{"app-server"}, nil) {
		t.Fatal("expected service fallback")
	}
	if isUpstreamServiceFallback([]string{"10.0.0.1"}, []LBEndpointSnapshot{{PodIP: "10.0.0.1"}}) {
		t.Fatal("expected pod IP upstream not to be fallback")
	}
}

func TestReconcileLBUpstream_rejectsServiceFallback(t *testing.T) {
	t.Parallel()
	ns := "cloud9-reconcile-fallback"
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "lb1", Type: "lb", Name: "LB"},
			{ID: "a1", Type: "app-server", Name: "App", Config: map[string]interface{}{"replicas": 3}},
		},
		Connections: []DeploymentConn{{SourceID: "lb1", TargetID: "a1"}},
	}
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: lbConfigMapName, Namespace: ns},
		Data:       map[string]string{"nginx.conf": "old"},
	})
	err := ReconcileLBUpstream(context.Background(), client, ns, design)
	if err == nil {
		t.Fatal("expected error when endpoints unavailable")
	}
	if !strings.Contains(err.Error(), "service fallback") && !strings.Contains(err.Error(), "endpoints") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReconcileLBUpstream_updatesConfigMap(t *testing.T) {
	t.Parallel()
	ns := "cloud9-reconcile-update"
	spec := DefaultLBSpec()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "lb1", Type: "lb", Name: "LB"},
			{ID: "a1", Type: "app-server", Name: "App", Config: map[string]interface{}{"replicas": 2}},
		},
		Connections: []DeploymentConn{{SourceID: "lb1", TargetID: "a1"}},
	}
	upstreamHosts := []string{"10.0.0.1", "10.0.0.2"}
	wantConf := spec.nginxConfig(upstreamHosts, "round-robin")

	client := fake.NewSimpleClientset(
		&corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
			Subsets: []corev1.EndpointSubset{{
				Addresses: []corev1.EndpointAddress{
					{IP: "10.0.0.1", TargetRef: &corev1.ObjectReference{Name: "app-server-0"}},
					{IP: "10.0.0.2", TargetRef: &corev1.ObjectReference{Name: "app-server-1"}},
				},
			}},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: lbConfigMapName, Namespace: ns},
			Data:       map[string]string{"nginx.conf": spec.nginxConfig([]string{"app-server"}, "round-robin")},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "lb", Namespace: ns},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr(int32(1)),
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "lb", "deployment": ns}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "lb", "deployment": ns}},
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lb-ready",
				Namespace: ns,
				Labels:    map[string]string{"app": "lb", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				}},
			},
		},
	)

	if err := ReconcileLBUpstream(context.Background(), client, ns, design); err != nil {
		t.Fatalf("ReconcileLBUpstream: %v", err)
	}

	cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), lbConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	if cm.Data["nginx.conf"] != wantConf {
		t.Fatalf("nginx.conf not updated to pod IPs\nwant contains 10.0.0.1\n got %s", cm.Data["nginx.conf"])
	}
	if cm.Annotations[lbUpstreamFallbackAnnotation] != "false" {
		t.Fatalf("fallback annotation = %q want false", cm.Annotations[lbUpstreamFallbackAnnotation])
	}
}
