package deployer

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDefaultLBSpec(t *testing.T) {
	t.Parallel()
	spec := DefaultLBSpec()
	if spec.Image != imageNginx {
		t.Fatalf("Image = %q want %q", spec.Image, imageNginx)
	}
	if spec.WorkerConnections != 4096 || spec.UpstreamKeepalive != 32 {
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
	if !strings.Contains(rr, "location /nginx_status") || !strings.Contains(rr, "stub_status") {
		t.Fatalf("round-robin config missing stub_status: %s", rr)
	}
	if strings.Contains(rr, "least_conn") || strings.Contains(rr, "ip_hash") {
		t.Fatalf("unexpected alg directives in round-robin: %s", rr)
	}

	lc := spec.nginxConfig(backends, "least_conn")
	if !strings.Contains(lc, "least_conn") {
		t.Fatalf("least_conn: %s", lc)
	}

	iph := spec.nginxConfig(backends, "ip_hash")
	if !strings.Contains(iph, "ip_hash") {
		t.Fatalf("ip_hash: %s", iph)
	}

	multi := spec.nginxConfig([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, "least_conn")
	if !strings.Contains(multi, "server 10.0.0.1:8080") || !strings.Contains(multi, "server 10.0.0.3:8080") {
		t.Fatalf("multi upstream: %s", multi)
	}
	if !strings.Contains(multi, "worker_connections 4096") {
		t.Fatalf("expected worker_connections 4096: %s", multi)
	}
	if !strings.Contains(multi, "keepalive 32") {
		t.Fatalf("expected upstream keepalive: %s", multi)
	}
	if !strings.Contains(multi, "proxy_http_version 1.1") || !strings.Contains(multi, "proxy_read_timeout 60s") {
		t.Fatalf("expected proxy tuning: %s", multi)
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
