package deployer

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDeployLoadgen_CreatesDeploymentAndService(t *testing.T) {
	t.Setenv("TEMPLATE_IMAGE_REGISTRY", "reg.example/test")

	cs := fake.NewClientset()
	ns := "cloud9-test-abc"
	if err := DeployLoadgen(context.Background(), cs, ns, "explorer"); err != nil {
		t.Fatalf("DeployLoadgen: %v", err)
	}

	dep, err := cs.AppsV1().Deployments(ns).Get(context.Background(), loadgenDeployName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	ctr := dep.Spec.Template.Spec.Containers[0]
	img := ctr.Image
	if img != "reg.example/test/loadgen:v1" && img != "reg.example/test/loadgen:git-e021199" {
		t.Fatalf("image: got %q", img)
	}
	if got := ctr.ImagePullPolicy; got != corev1.PullIfNotPresent {
		t.Fatalf("imagePullPolicy: got %q want %q", got, corev1.PullIfNotPresent)
	}
	req := dep.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]
	if req.String() != "250m" {
		t.Fatalf("cpu request: got %s want 250m", req.String())
	}
	mem := dep.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]
	if mem.String() != "256Mi" {
		t.Fatalf("memory request: got %s want 256Mi", mem.String())
	}

	svc, err := cs.CoreV1().Services(ns).Get(context.Background(), loadgenService, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	if svc.Spec.Ports[0].Port != loadgenPort {
		t.Fatalf("service port: got %d want %d", svc.Spec.Ports[0].Port, loadgenPort)
	}
}

func TestDeployLoadgen_EngineerTierResources(t *testing.T) {
	t.Parallel()
	res := loadgenResources("engineer")
	cpu := res.Limits[corev1.ResourceCPU]
	mem := res.Limits[corev1.ResourceMemory]
	if cpu.String() != "500m" {
		t.Fatalf("engineer cpu: got %s want 500m", cpu.String())
	}
	if mem.String() != "512Mi" {
		t.Fatalf("engineer memory: got %s want 512Mi", mem.String())
	}
}

func TestDeployLoadgen_InternalTierResources(t *testing.T) {
	t.Parallel()
	res := loadgenResources("internal")
	cpu := res.Limits[corev1.ResourceCPU]
	mem := res.Limits[corev1.ResourceMemory]
	if cpu.Cmp(resource.MustParse("1000m")) != 0 {
		t.Fatalf("internal cpu: got %s want 1000m", cpu.String())
	}
	if mem.String() != "1Gi" {
		t.Fatalf("internal memory: got %s want 1Gi", mem.String())
	}
}

func TestDeployLoadgen_Idempotent(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset()
	ns := "cloud9-idem"
	if err := DeployLoadgen(context.Background(), cs, ns, "explorer"); err != nil {
		t.Fatal(err)
	}
	if err := DeployLoadgen(context.Background(), cs, ns, "explorer"); err != nil {
		t.Fatalf("second DeployLoadgen: %v", err)
	}
	list, _ := cs.AppsV1().Deployments(ns).List(context.Background(), metav1.ListOptions{})
	if len(list.Items) != 1 {
		t.Fatalf("deployment count: got %d want 1", len(list.Items))
	}
	_ = appsv1.Deployment{}
}
