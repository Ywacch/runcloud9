package deployer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCacheImage_UsesTemplateImageTag(t *testing.T) {
	t.Setenv("TEMPLATE_IMAGE_REGISTRY", "reg.example/test")
	t.Setenv("TEMPLATE_IMAGE_TAG", "git-abc1234")
	got := cacheImage("write-through")
	want := "reg.example/test/caching-proxy:git-abc1234"
	if got != want {
		t.Fatalf("cacheImage(write-through): got %q want %q", got, want)
	}
}

func TestNewCacheProxyContainer_SetsPullAlways(t *testing.T) {
	c := newCacheProxyContainer("example/cache-proxy:tag", DefaultResources(), nil)
	if got := c.ImagePullPolicy; got != corev1.PullAlways {
		t.Fatalf("cache-proxy imagePullPolicy: got %q want %q", got, corev1.PullAlways)
	}
	if c.Name != "cache-proxy" {
		t.Fatalf("container name: got %q want cache-proxy", c.Name)
	}
	if c.ReadinessProbe == nil || c.ReadinessProbe.HTTPGet == nil {
		t.Fatal("expected HTTP readiness probe for cache-proxy")
	}
	if c.ReadinessProbe.TimeoutSeconds != 5 || c.ReadinessProbe.PeriodSeconds != 5 {
		t.Fatalf("cache-proxy readiness: TimeoutSeconds=%d PeriodSeconds=%d want 5/5",
			c.ReadinessProbe.TimeoutSeconds, c.ReadinessProbe.PeriodSeconds)
	}
	if c.ReadinessProbe.FailureThreshold != 8 {
		t.Fatalf("cache-proxy readiness FailureThreshold: got %d want 8", c.ReadinessProbe.FailureThreshold)
	}
}

func TestK8sDeployer_Deploy_CreatesNamespaceAndDeployment(t *testing.T) {
	// UNDERSTAND: fake.NewSimpleClientset() is an in-memory K8s API.
	// Create/Delete/Get calls work but no pods actually run.
	// This lets us test the client-go calls without minikube.
	fakeClient := fake.NewSimpleClientset()
	d := NewK8sDeployer(fakeClient, "db")

	cfg := DeployConfig{
		Namespace: "cloud9-test-123",
		Node:      DeploymentNode{ID: "n1", Type: "db", Name: "PostgreSQL"},
	}

	// waitForReady will block forever against a fake client because no pod events fire.
	// Use a short-deadline context and assert that the error is a timeout (not a logic error).
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := d.Deploy(ctx, cfg)
	if err == nil {
		t.Fatal("expected error from waitForReady timeout, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded (or wrapped), got %v", err)
	}

	ns, getErr := fakeClient.CoreV1().Namespaces().Get(
		context.Background(), cfg.Namespace, metav1.GetOptions{},
	)
	if getErr != nil {
		t.Fatalf("namespace not found: %v", getErr)
	}
	if ns.Name != cfg.Namespace {
		t.Errorf("expected namespace %s, got %s", cfg.Namespace, ns.Name)
	}

	policies, npErr := fakeClient.NetworkingV1().NetworkPolicies(cfg.Namespace).List(context.Background(), metav1.ListOptions{})
	if npErr != nil {
		t.Fatalf("list network policies: %v", npErr)
	}
	if len(policies.Items) < 2 {
		t.Fatalf("expected at least 2 network policies, got %d", len(policies.Items))
	}
	deny, err := fakeClient.NetworkingV1().NetworkPolicies(cfg.Namespace).Get(context.Background(), defaultDenyEgressPolicyName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get default deny egress policy: %v", err)
	}
	if len(deny.Spec.PolicyTypes) != 1 || deny.Spec.PolicyTypes[0] != networkingv1.PolicyTypeEgress {
		t.Fatalf("default deny policy types: got %+v, want [Egress]", deny.Spec.PolicyTypes)
	}
	if len(deny.Spec.Egress) != 0 {
		t.Fatalf("default deny policy should have zero egress rules, got %d", len(deny.Spec.Egress))
	}

	allow, err := fakeClient.NetworkingV1().NetworkPolicies(cfg.Namespace).Get(context.Background(), allowDNSAndNamespaceEgressPolicy, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get allow policy: %v", err)
	}
	if len(allow.Spec.Egress) < 2 {
		t.Fatalf("allow policy egress rules: got %d, want >=2", len(allow.Spec.Egress))
	}
}

func TestK8sDeployer_Teardown_DeletesNamespace(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	d := NewK8sDeployer(fakeClient, "db")

	ns := "cloud9-test-456"
	_, err := fakeClient.CoreV1().Namespaces().Create(
		context.Background(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatalf("pre-create namespace: %v", err)
	}

	err = d.Teardown(context.Background(), ns)
	if err != nil {
		t.Fatalf("teardown failed: %v", err)
	}

	_, err = fakeClient.CoreV1().Namespaces().Get(
		context.Background(), ns, metav1.GetOptions{},
	)
	if err == nil {
		t.Error("expected namespace to be deleted, but it still exists")
	}
}

func TestLbBackends(t *testing.T) {
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

func TestGenerateNginxConfig(t *testing.T) {
	backends := []string{"app-server"}
	rr := generateNginxConfig(backends, "round-robin")
	if !strings.Contains(rr, "upstream backends") || !strings.Contains(rr, "proxy_pass http://backends") {
		t.Fatalf("round-robin config: %s", rr)
	}
	if !strings.Contains(rr, "location /nginx_status") || !strings.Contains(rr, "stub_status") {
		t.Fatalf("round-robin config missing stub_status: %s", rr)
	}
	if strings.Contains(rr, "least_conn") || strings.Contains(rr, "ip_hash") {
		t.Fatalf("unexpected alg directives in round-robin: %s", rr)
	}

	lc := generateNginxConfig(backends, "least_conn")
	if !strings.Contains(lc, "least_conn") {
		t.Fatalf("least_conn: %s", lc)
	}

	iph := generateNginxConfig(backends, "ip_hash")
	if !strings.Contains(iph, "ip_hash") {
		t.Fatalf("ip_hash: %s", iph)
	}
}
