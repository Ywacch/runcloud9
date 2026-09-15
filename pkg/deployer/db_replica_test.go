package deployer

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

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

func TestDBReplicaStatefulSetSpec_readReplicaShape(t *testing.T) {
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
	sts := dbReplicaStatefulSetSpec(cfg, DBResources(), 1, "small")

	if sts.Name != dbReplicaStatefulSetName {
		t.Fatalf("statefulset name: got %q want %q", sts.Name, dbReplicaStatefulSetName)
	}
	if sts.Namespace != cfg.Namespace {
		t.Fatalf("namespace: got %q want %q", sts.Namespace, cfg.Namespace)
	}
	if sts.Spec.ServiceName != dbReplicaHeadlessServiceName {
		t.Fatalf("serviceName: got %q want %q", sts.Spec.ServiceName, dbReplicaHeadlessServiceName)
	}
	if got := sts.Spec.Selector.MatchLabels["app"]; got != dbReplicaAppLabel {
		t.Fatalf("selector app: got %q want %q", got, dbReplicaAppLabel)
	}
	if got := sts.Spec.Template.Labels["app"]; got != dbReplicaAppLabel {
		t.Fatalf("pod label app: got %q want %q", got, dbReplicaAppLabel)
	}

	podSpec := sts.Spec.Template.Spec
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
	for _, want := range []string{"pg_isready -h db -U syscrack", "pg_basebackup", "-h db", "-U syscrack", "--checkpoint=fast", "application_name=${HOSTNAME}"} {
		if !strings.Contains(initCmd, want) {
			t.Fatalf("init command missing %q: %q", want, initCmd)
		}
	}
	if strings.Contains(initCmd, "exec pg_basebackup") {
		t.Fatalf("init must not exec pg_basebackup so primary_conninfo can be appended: %q", initCmd)
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

func TestDBPrimaryResources_ephemeralStorage(t *testing.T) {
	t.Parallel()

	base := dbResourcesForSize("small")
	got := dbPrimaryResources(base)
	wantEphemeral := resource.MustParse(dbReplicaEphemeralSize)
	assertEphemeralStorage(t, got, wantEphemeral)
}

func TestDBReplicaHeadlessServiceSpec_clusterIPNone(t *testing.T) {
	t.Parallel()

	cfg := DeployConfig{Namespace: "cloud9-test-ns"}
	svc := dbReplicaHeadlessServiceSpec(cfg)
	if svc.Name != dbReplicaHeadlessServiceName {
		t.Fatalf("service name: got %q want %q", svc.Name, dbReplicaHeadlessServiceName)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("clusterIP: got %q want None", svc.Spec.ClusterIP)
	}
	if got := svc.Spec.Selector["app"]; got != dbReplicaAppLabel {
		t.Fatalf("selector app: got %q want %q", got, dbReplicaAppLabel)
	}
}

func TestDBReplicaServiceSpec_selectorAndPort(t *testing.T) {
	t.Parallel()

	cfg := DeployConfig{Namespace: "cloud9-test-ns"}
	svc := dbReplicaServiceSpec(cfg)
	if svc.Name != DBReplicaServiceName {
		t.Fatalf("service name: got %q want %q", svc.Name, DBReplicaServiceName)
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

func TestDBReplicaStatefulSetSpec_usesStandbyInstanceSize(t *testing.T) {
	t.Parallel()

	cfg := DeployConfig{
		Namespace: "cloud9-test-ns",
		Node: DeploymentNode{
			ID:   "db1",
			Type: "db",
			Name: "PostgreSQL",
			Config: map[string]any{
				"readReplicas":  1,
				"instanceSizes": []any{"large", "small"},
			},
		},
	}
	large := dbResourcesForSize("large")
	small := dbResourcesForSize("small")
	sts := dbReplicaStatefulSetSpec(cfg, small, 1, "small")

	replicaCPU := sts.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu()
	if replicaCPU.Cmp(*large.Limits.Cpu()) == 0 {
		t.Fatalf("replica CPU limit should follow instanceSizes[1]=small, not primary large")
	}
	if replicaCPU.Cmp(*small.Limits.Cpu()) != 0 {
		t.Fatalf("replica CPU limit %s should match small %s", replicaCPU, small.Limits.Cpu())
	}
}

func TestDBReplicaStatefulSetSpec_mixedStandbySizesUseSizeSuffix(t *testing.T) {
	t.Parallel()

	cfg := DeployConfig{
		Namespace: "cloud9-test-ns",
		Node: DeploymentNode{
			Config: map[string]any{
				"readReplicas":  2,
				"instanceSizes": []any{"large", "large", "small"},
			},
		},
	}
	groups := sizeGroups([]string{"large", "small"})
	if len(groups) != 2 {
		t.Fatalf("size groups: got %d want 2", len(groups))
	}
	largeSTS := dbReplicaStatefulSetSpecNamed(cfg, dbReplicaStatefulSetName, dbResourcesForSize("large"), groups[0].Count, "large")
	smallSTS := dbReplicaStatefulSetSpecNamed(cfg, dbReplicaStatefulSetName+"-small", dbResourcesForSize("small"), groups[1].Count, "small")
	if largeSTS.Name != dbReplicaStatefulSetName {
		t.Fatalf("large sts name: got %q", largeSTS.Name)
	}
	if smallSTS.Name != "postgres-replica-small" {
		t.Fatalf("small sts name: got %q", smallSTS.Name)
	}
	if *largeSTS.Spec.Replicas != 1 || *smallSTS.Spec.Replicas != 1 {
		t.Fatalf("replica counts: large=%d small=%d", *largeSTS.Spec.Replicas, *smallSTS.Spec.Replicas)
	}
}

func TestDBReplicaHostnames_uniformSizes(t *testing.T) {
	t.Parallel()

	got := dbReplicaHostnames(2, []string{"large", "large"})
	want := []string{"postgres-replica-0", "postgres-replica-1"}
	if len(got) != len(want) {
		t.Fatalf("hosts: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("hosts[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestDBReplicaHostnames_mixedSizeGroups(t *testing.T) {
	t.Parallel()

	got := dbReplicaHostnames(2, []string{"large", "small"})
	want := []string{"postgres-replica-0", "postgres-replica-small-0"}
	if len(got) != len(want) {
		t.Fatalf("hosts: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("hosts[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestDBReplicaHostsFromConfig(t *testing.T) {
	t.Parallel()

	cfg := map[string]any{
		"readReplicas":  2,
		"instanceSizes": []any{"large", "large", "large"},
	}
	if got := dbReplicaHostsFromConfig(cfg); got != "postgres-replica-0,postgres-replica-1" {
		t.Fatalf("hosts: got %q", got)
	}
}

func TestDBReplicaLayerIDMatchesOrdinalHostname(t *testing.T) {
	t.Parallel()
	// StatefulSet ordinal-0 keeps the same layer id after pod kill/recreate.
	const hostname = "postgres-replica-0"
	if hostname != dbReplicaHostnames(1, []string{"large"})[0] {
		t.Fatalf("layer id hostname mismatch")
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
