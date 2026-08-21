package deployer

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestPostgresPrimaryCommand_syncIncludesStandbyNames(t *testing.T) {
	t.Parallel()

	cfg := map[string]any{
		"replicationMode": "sync",
		"readReplicas":    1,
	}
	cmd := strings.Join(postgresPrimaryCommand(cfg), " ")
	for _, want := range []string{"synchronous_commit=on", "synchronous_standby_names=*"} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("command missing %q: %q", want, cmd)
		}
	}
}

func TestPostgresPrimaryCommand_asyncOmitsStandbyNames(t *testing.T) {
	t.Parallel()

	cfg := map[string]any{
		"replicationMode": "async",
		"readReplicas":    1,
	}
	cmd := strings.Join(postgresPrimaryCommand(cfg), " ")
	if strings.Contains(cmd, "synchronous_standby_names") {
		t.Fatalf("async command should not set synchronous_standby_names: %q", cmd)
	}
}

func TestPostgresPrimaryCommand_syncWithoutReplicaOmitsStandbyNames(t *testing.T) {
	t.Parallel()

	cfg := map[string]any{
		"replicationMode": "sync",
		"readReplicas":    0,
	}
	cmd := strings.Join(postgresPrimaryCommand(cfg), " ")
	if strings.Contains(cmd, "synchronous_standby_names") {
		t.Fatalf("sync with zero read replicas should not set standby names: %q", cmd)
	}
}

func TestDBReplicaDeploymentSpec_readReplicaShape(t *testing.T) {
	t.Parallel()

	cfg := DeployConfig{
		Namespace: "cloud9-test-ns",
		Node: DeploymentNode{
			ID:   "db1",
			Type: "db",
			Name: "PostgreSQL",
			Config: map[string]any{
				"readReplicas":    1,
				"replicationMode": "async",
			},
		},
	}
	dep := dbReplicaDeploymentSpec(cfg, DBResources(), 1)

	if dep.Name != dbReplicaDeploymentName {
		t.Fatalf("deployment name: got %q want %q", dep.Name, dbReplicaDeploymentName)
	}
	if dep.Namespace != cfg.Namespace {
		t.Fatalf("namespace: got %q want %q", dep.Namespace, cfg.Namespace)
	}
	if got := dep.Spec.Selector.MatchLabels["app"]; got != dbReplicaAppLabel {
		t.Fatalf("selector app: got %q want %q", got, dbReplicaAppLabel)
	}
	if got := dep.Spec.Selector.MatchLabels["deployment"]; got != cfg.Namespace {
		t.Fatalf("selector deployment: got %q want %q", got, cfg.Namespace)
	}
	if got := dep.Spec.Template.Labels["app"]; got != dbReplicaAppLabel {
		t.Fatalf("pod label app: got %q want %q", got, dbReplicaAppLabel)
	}
	if got := dep.Spec.Template.Labels["deployment"]; got != cfg.Namespace {
		t.Fatalf("pod label deployment: got %q want %q", got, cfg.Namespace)
	}

	podSpec := dep.Spec.Template.Spec
	if len(podSpec.Volumes) != 1 || podSpec.Volumes[0].EmptyDir == nil {
		t.Fatal("expected one emptyDir volume for PGDATA")
	}
	if podSpec.Volumes[0].EmptyDir.SizeLimit == nil {
		t.Fatal("expected emptyDir sizeLimit for Autopilot")
	}
	wantEphemeral := resource.MustParse(dbReplicaEphemeralSize)
	if got := *podSpec.Volumes[0].EmptyDir.SizeLimit; got.Cmp(wantEphemeral) != 0 {
		t.Fatalf("emptyDir sizeLimit: got %s want %s", got.String(), wantEphemeral.String())
	}

	if len(podSpec.InitContainers) != 1 {
		t.Fatalf("init containers: got %d want 1", len(podSpec.InitContainers))
	}
	initCmd := strings.Join(podSpec.InitContainers[0].Command, " ")
	for _, want := range []string{"pg_isready", "pg_basebackup", "-h db", "-U syscrack"} {
		if !strings.Contains(initCmd, want) {
			t.Fatalf("init command missing %q: %q", want, initCmd)
		}
	}
	assertEphemeralStorage(t, podSpec.InitContainers[0].Resources, wantEphemeral)

	if len(podSpec.Containers) != 2 {
		t.Fatalf("containers: got %d want 2 (postgres + exporter)", len(podSpec.Containers))
	}
	if podSpec.Containers[0].Name != dbReplicaAppLabel {
		t.Fatalf("main container name: got %q want %q", podSpec.Containers[0].Name, dbReplicaAppLabel)
	}
	assertEphemeralStorage(t, podSpec.Containers[0].Resources, wantEphemeral)
}

func TestDBReplicaServiceSpec_selectorAndPort(t *testing.T) {
	t.Parallel()

	cfg := DeployConfig{Namespace: "cloud9-test-ns"}
	svc := dbReplicaServiceSpec(cfg)
	if svc.Name != dbReplicaServiceName {
		t.Fatalf("service name: got %q want %q", svc.Name, dbReplicaServiceName)
	}
	if got := svc.Spec.Selector["app"]; got != dbReplicaAppLabel {
		t.Fatalf("selector app: got %q want %q", got, dbReplicaAppLabel)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 5432 {
		t.Fatalf("service ports: %+v", svc.Spec.Ports)
	}
}

func TestDBReadReplicaCount_engineGate(t *testing.T) {
	t.Parallel()

	cfg := DeployConfig{
		Node: DeploymentNode{
			Type: "db",
			Config: map[string]any{
				"engine":       "mysql",
				"readReplicas": 1,
			},
		},
	}
	if got := dbReadReplicaCount(cfg); got != 0 {
		t.Fatalf("mysql engine should not deploy read replicas, got %d", got)
	}
}

func assertEphemeralStorage(t *testing.T, res corev1.ResourceRequirements, want resource.Quantity) {
	t.Helper()
	if lim := res.Limits[corev1.ResourceEphemeralStorage]; lim.Cmp(want) != 0 {
		t.Fatalf("ephemeral-storage limit: got %s want %s", lim.String(), want.String())
	}
	if req := res.Requests[corev1.ResourceEphemeralStorage]; req.Cmp(want) != 0 {
		t.Fatalf("ephemeral-storage request: got %s want %s", req.String(), want.String())
	}
}
