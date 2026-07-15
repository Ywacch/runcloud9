package deployer

import (
	"context"
	"errors"
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
	gotRT := cacheImage("read-through")
	if gotRT != want {
		t.Fatalf("cacheImage(read-through): got %q want %q (legacy fold)", gotRT, want)
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
	if got := c.ReadinessProbe.HTTPGet.Path; got != "/health/ready" {
		t.Fatalf("cache-proxy readiness path: got %q want /health/ready", got)
	}
	if c.ReadinessProbe.TimeoutSeconds != 5 || c.ReadinessProbe.PeriodSeconds != 5 {
		t.Fatalf("cache-proxy readiness: TimeoutSeconds=%d PeriodSeconds=%d want 5/5",
			c.ReadinessProbe.TimeoutSeconds, c.ReadinessProbe.PeriodSeconds)
	}
	if c.ReadinessProbe.FailureThreshold != 8 {
		t.Fatalf("cache-proxy readiness FailureThreshold: got %d want 8", c.ReadinessProbe.FailureThreshold)
	}
	if c.LivenessProbe == nil || c.LivenessProbe.HTTPGet == nil {
		t.Fatal("expected HTTP liveness probe for cache-proxy")
	}
	if got := c.LivenessProbe.HTTPGet.Path; got != "/health/live" {
		t.Fatalf("cache-proxy liveness path: got %q want /health/live", got)
	}
}

func TestTemplateProbes_SplitPaths(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		probe *corev1.Probe
		path  string
	}{
		{"ready", templateReadyProbe(8080, 3, 5, 8, 3), "/health/ready"},
		{"live", templateLiveProbe(8080), "/health/live"},
		{"legacy-health", templateHTTPReadinessProbe(8080, 3, 5, 8, 5), "/health"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.probe == nil || tc.probe.HTTPGet == nil {
				t.Fatalf("%s: expected HTTP probe", tc.name)
			}
			if got := tc.probe.HTTPGet.Path; got != tc.path {
				t.Fatalf("%s path: got %q want %q", tc.name, got, tc.path)
			}
			if tc.probe.HTTPGet.Port.IntVal != 8080 {
				t.Fatalf("%s port: got %d want 8080", tc.name, tc.probe.HTTPGet.Port.IntVal)
			}
		})
	}
}

func TestTemplateLiveProbe_ConservativeThresholds(t *testing.T) {
	t.Parallel()
	p := templateLiveProbe(8081)
	if p.FailureThreshold != 6 {
		t.Fatalf("liveness FailureThreshold: got %d want 6", p.FailureThreshold)
	}
	if p.PeriodSeconds != 10 || p.InitialDelaySeconds != 10 {
		t.Fatalf("liveness timing: period=%d initialDelay=%d want 10/10",
			p.PeriodSeconds, p.InitialDelaySeconds)
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
	dnsRule := allow.Spec.Egress[0]
	foundNodeLocalDNS := false
	for _, peer := range dnsRule.To {
		if peer.IPBlock != nil && peer.IPBlock.CIDR == "169.254.20.10/32" {
			foundNodeLocalDNS = true
			break
		}
	}
	if !foundNodeLocalDNS {
		t.Fatalf("allow policy DNS rule missing NodeLocal DNS IP block 169.254.20.10/32")
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

func TestWaitForReady_SingleReplicaFromList(t *testing.T) {
	ns := "cloud9-test-one"
	labels := map[string]string{"app": "app-server", "deployment": ns}
	fakeClient := fake.NewSimpleClientset()
	ctx := context.Background()
	pod, err := fakeClient.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server-0", Namespace: ns, Labels: labels},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pod: %v", err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := fakeClient.CoreV1().Pods(ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update status: %v", err)
	}
	d := NewK8sDeployer(fakeClient, "db")
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.waitForReady(waitCtx, ns, "app-server", 1); err != nil {
		t.Fatalf("waitForReady(1): %v", err)
	}
}

func TestPodConditionReady_CountsMultiple(t *testing.T) {
	t.Parallel()
	var ready int
	for i := 0; i < 3; i++ {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}
		if podConditionReady(pod) {
			ready++
		}
	}
	if ready != 3 {
		t.Fatalf("ready count = %d want 3", ready)
	}
}
