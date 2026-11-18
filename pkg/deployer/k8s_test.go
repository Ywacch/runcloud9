package deployer

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

func TestNewCacheProxyContainer_SetsIfNotPresent(t *testing.T) {
	c := newCacheProxyContainer("example/cache-proxy:tag", DefaultResources(), nil)
	if got := c.ImagePullPolicy; got != corev1.PullIfNotPresent {
		t.Fatalf("cache-proxy imagePullPolicy: got %q want %q", got, corev1.PullIfNotPresent)
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

func TestDeploy_SetsExpiryLabel(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	d := NewK8sDeployer(fakeClient, "db")
	expires := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	cfg := DeployConfig{
		Namespace: "cloud9-expiry",
		Node:      DeploymentNode{ID: "n1", Type: "db", Name: "PostgreSQL"},
		ExpiresAt: expires,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = d.Deploy(ctx, cfg)

	ns, err := fakeClient.CoreV1().Namespaces().Get(context.Background(), cfg.Namespace, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get namespace: %v", err)
	}
	got := ns.Labels[NamespaceExpiresAtLabel]
	want := namespaceExpiresAtLabelValue(expires)
	if got != want {
		t.Fatalf("label = %q want %q", got, want)
	}
}

func TestDeploy_SetsExpiryLabel_ZeroSkipsLabel(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	d := NewK8sDeployer(fakeClient, "db")
	cfg := DeployConfig{
		Namespace: "cloud9-no-expiry",
		Node:      DeploymentNode{ID: "n1", Type: "db", Name: "PostgreSQL"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = d.Deploy(ctx, cfg)

	ns, err := fakeClient.CoreV1().Namespaces().Get(context.Background(), cfg.Namespace, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get namespace: %v", err)
	}
	if _, ok := ns.Labels[NamespaceExpiresAtLabel]; ok {
		t.Fatalf("unexpected label on namespace: %v", ns.Labels)
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

func TestWaitForReady_AlreadyReady(t *testing.T) {
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
	cfg := DeployConfig{Namespace: ns, Node: DeploymentNode{ID: "app-1"}}
	if err := d.waitForReady(waitCtx, cfg, "app-server", 1, waitProgressOpts{}); err != nil {
		t.Fatalf("waitForReady(1): %v", err)
	}
}

func TestWaitForReady_BecomesReadyOnLaterPoll(t *testing.T) {
	ns := "cloud9-test-poll-ready"
	labels := map[string]string{"app": "app-server", "deployment": ns}
	fakeClient := fake.NewSimpleClientset()
	ctx := context.Background()
	pod, err := fakeClient.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server-0", Namespace: ns, Labels: labels},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pod: %v", err)
	}

	d := NewK8sDeployer(fakeClient, "db")
	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg := DeployConfig{Namespace: ns, Node: DeploymentNode{ID: "app-1"}}

	errCh := make(chan error, 1)
	go func() {
		errCh <- d.waitForReady(waitCtx, cfg, "app-server", 1, waitProgressOpts{})
	}()

	time.Sleep(200 * time.Millisecond)
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := fakeClient.CoreV1().Pods(ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update status: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("waitForReady: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waitForReady did not return after pod became Ready")
	}
}

func TestWaitForReady_Timeout(t *testing.T) {
	t.Parallel()
	ns := "cloud9-test-timeout"
	labels := map[string]string{"app": "app-server", "deployment": ns}
	fakeClient := fake.NewSimpleClientset()
	ctx := context.Background()
	if _, err := fakeClient.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server-0", Namespace: ns, Labels: labels},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	d := NewK8sDeployer(fakeClient, "db")
	waitCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	cfg := DeployConfig{Namespace: ns, Node: DeploymentNode{ID: "app-1"}}
	err := d.waitForReady(waitCtx, cfg, "app-server", 1, waitProgressOpts{})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestWaitForReady_ProgressMixedReady(t *testing.T) {
	ns := "cloud9-test-progress"
	labels := map[string]string{"app": "app-server", "deployment": ns}
	fakeClient := fake.NewSimpleClientset()
	ctx := context.Background()

	readyPod, err := fakeClient.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server-0", Namespace: ns, Labels: labels},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create ready pod: %v", err)
	}
	readyPod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := fakeClient.CoreV1().Pods(ns).UpdateStatus(ctx, readyPod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update ready pod status: %v", err)
	}

	pullingPod, err := fakeClient.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server-1", Namespace: ns, Labels: labels},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pulling pod: %v", err)
	}
	pullingPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "app",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
	}}
	if _, err := fakeClient.CoreV1().Pods(ns).UpdateStatus(ctx, pullingPod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update pulling pod status: %v", err)
	}

	var last []PodWaitProgress
	cfg := DeployConfig{
		Namespace: ns,
		Node:      DeploymentNode{ID: "app-1"},
		OnWaitProgress: func(_ string, pods []PodWaitProgress) {
			copied := make([]PodWaitProgress, len(pods))
			copy(copied, pods)
			last = copied
		},
	}

	d := NewK8sDeployer(fakeClient, "db")
	waitCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = d.waitForReady(waitCtx, cfg, "app-server", 2, waitProgressOpts{})

	if len(last) != 2 {
		t.Fatalf("progress pods: got %d want 2", len(last))
	}
	if !last[0].Ready || last[0].WaitCode != WaitCodeReady {
		t.Fatalf("first pod: got ready=%v wait_code=%q want ready=true wait_code=ready", last[0].Ready, last[0].WaitCode)
	}
	if last[1].Ready || last[1].WaitCode != WaitCodePullingImage {
		t.Fatalf("second pod: got ready=%v wait_code=%q want ready=false wait_code=pulling_image", last[1].Ready, last[1].WaitCode)
	}
}

func TestTeardown_NotFoundIsIdempotent(t *testing.T) {
	t.Parallel()
	fakeClient := fake.NewSimpleClientset()
	d := NewK8sDeployer(fakeClient, "db")
	if err := d.Teardown(context.Background(), "missing-ns"); err != nil {
		t.Fatalf("Teardown missing namespace: %v", err)
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

func assertDeploymentRevisionHistoryLimitZero(t *testing.T, dep *appsv1.Deployment) {
	t.Helper()
	if dep == nil || dep.Spec.RevisionHistoryLimit == nil || *dep.Spec.RevisionHistoryLimit != 0 {
		var got any
		if dep != nil && dep.Spec.RevisionHistoryLimit != nil {
			got = *dep.Spec.RevisionHistoryLimit
		}
		name := ""
		if dep != nil {
			name = dep.Name
		}
		t.Fatalf("deployment %q revisionHistoryLimit: got %v want 0", name, got)
	}
}

func TestDeploy_RevisionHistoryLimitZero(t *testing.T) {
	design := urlShortenerDefaultDesign()
	ns := "cloud9-rev-hist"
	fakeClient := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var dbNode, appNode DeploymentNode
	for _, n := range design.Nodes {
		switch n.Type {
		case "db":
			dbNode = n
		case "app-server":
			appNode = n
		}
	}

	dDB := NewK8sDeployer(fakeClient, "db")
	_ = dDB.Deploy(ctx, DeployConfig{Namespace: ns, Node: dbNode, Design: design})
	dbDep, err := fakeClient.AppsV1().Deployments(ns).Get(context.Background(), "postgres", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get postgres deployment: %v", err)
	}
	assertDeploymentRevisionHistoryLimitZero(t, dbDep)

	dApp := NewK8sDeployer(fakeClient, "app-server")
	_ = dApp.Deploy(ctx, DeployConfig{Namespace: ns, Node: appNode, Design: design})
	appDep, err := fakeClient.AppsV1().Deployments(ns).Get(context.Background(), "app-server", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get app-server deployment: %v", err)
	}
	assertDeploymentRevisionHistoryLimitZero(t, appDep)
}

func stubWeightDefaultDesign() DeploymentDesign {
	d := DeploymentDesign{
		Template:  "stub-weight",
		Archetype: "stub-weight",
		Nodes: []DeploymentNode{
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "App Server",
				Config: map[string]interface{}{
					"replicas":     1,
					"instanceSize": "small",
				},
			},
		},
	}
	NormalizeDesign(&d)
	return d
}

func TestDeploy_StubWeightSkipsDatabase(t *testing.T) {
	design := stubWeightDefaultDesign()
	ns := "cloud9-stub-weight"
	fakeClient := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var appNode DeploymentNode
	for _, n := range design.Nodes {
		if n.Type == "app-server" {
			appNode = n
		}
	}

	d := NewK8sDeployer(fakeClient, "app-server")
	_ = d.Deploy(ctx, DeployConfig{Namespace: ns, Node: appNode, Design: design})

	if _, err := fakeClient.AppsV1().Deployments(ns).Get(context.Background(), "app-server", metav1.GetOptions{}); err != nil {
		t.Fatalf("app-server deployment: %v", err)
	}
	_, pgErr := fakeClient.AppsV1().Deployments(ns).Get(context.Background(), "postgres", metav1.GetOptions{})
	if !apierrors.IsNotFound(pgErr) {
		t.Fatalf("postgres deployment: got %v want NotFound", pgErr)
	}

	policies, err := fakeClient.NetworkingV1().NetworkPolicies(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list network policies: %v", err)
	}
	if len(policies.Items) < 2 {
		t.Fatalf("expected baseline network policies, got %d", len(policies.Items))
	}
	if _, err := fakeClient.CoreV1().Services(ns).Get(context.Background(), "app-server", metav1.GetOptions{}); err != nil {
		t.Fatalf("app-server service: %v", err)
	}
}

func TestDeploy_KeepsReaderServices(t *testing.T) {
	design := urlShortenerDefaultDesign()
	ns := "cloud9-svc-readers"
	fakeClient := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var dbNode, appNode DeploymentNode
	for _, n := range design.Nodes {
		switch n.Type {
		case "db":
			dbNode = n
		case "app-server":
			appNode = n
		}
	}

	dDB := NewK8sDeployer(fakeClient, "db")
	_ = dDB.Deploy(ctx, DeployConfig{Namespace: ns, Node: dbNode, Design: design})
	if _, err := fakeClient.CoreV1().Services(ns).Get(context.Background(), "db", metav1.GetOptions{}); err != nil {
		t.Fatalf("db service: %v", err)
	}

	dApp := NewK8sDeployer(fakeClient, "app-server")
	_ = dApp.Deploy(ctx, DeployConfig{Namespace: ns, Node: appNode, Design: design})
	if _, err := fakeClient.CoreV1().Services(ns).Get(context.Background(), "app-server", metav1.GetOptions{}); err != nil {
		t.Fatalf("app-server service: %v", err)
	}
}
