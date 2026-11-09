package deployer

import (
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestPostgresExporterQueriesEnabled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  DeployConfig
		want bool
	}{
		{
			name: "readReplicas one",
			cfg: DeployConfig{
				Node: DeploymentNode{
					Type: "db",
					Config: map[string]any{
						"readReplicas": 1,
					},
				},
			},
			want: true,
		},
		{
			name: "timeline on db postgresql",
			cfg: DeployConfig{
				Design: DeploymentDesign{
					Template: "social-feed",
					Nodes: []DeploymentNode{
						{ID: "db-1", Type: "db", Config: map[string]any{"engine": "postgresql"}},
					},
					JobBindings: map[string]string{"timeline": "db-1"},
				},
				Node: DeploymentNode{Type: "db", ID: "db-1"},
			},
			want: true,
		},
		{
			name: "readReplicas zero",
			cfg: DeployConfig{
				Node: DeploymentNode{
					Type: "db",
					Config: map[string]any{
						"readReplicas": 0,
					},
				},
			},
			want: false,
		},
		{
			name: "non db node",
			cfg: DeployConfig{
				Node: DeploymentNode{
					Type: "cache",
					Config: map[string]any{
						"readReplicas": 1,
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := postgresExporterQueriesEnabled(tt.cfg); got != tt.want {
				t.Fatalf("postgresExporterQueriesEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPostgresExporterQueriesConfigMap_content(t *testing.T) {
	t.Parallel()
	cm := postgresExporterQueriesConfigMap("cloud9-test-ns")
	if cm.Name != postgresExporterQueriesConfigMapName {
		t.Fatalf("configmap name: got %q want %q", cm.Name, postgresExporterQueriesConfigMapName)
	}
	primaryYAML, ok := cm.Data[postgresExporterQueriesFileName]
	if !ok {
		t.Fatalf("missing key %q", postgresExporterQueriesFileName)
	}
	for _, want := range []string{
		"replication_replay_lag:",
		"replay_lag_seconds",
		"replay_lag_bytes",
		"application_name",
		"sync_state",
		"master: true",
		"wal_lsn:",
		"pg_current_wal_lsn()",
		"c9_timeline:",
		"relation_bytes",
		"dead_tuples",
		"relname = 'timelines'",
	} {
		if !strings.Contains(primaryYAML, want) {
			t.Fatalf("primary queries yaml missing %q:\n%s", want, primaryYAML)
		}
	}
	if strings.Contains(primaryYAML, "wal_replay_lsn:") {
		t.Fatalf("primary queries yaml must not include wal_replay_lsn:\n%s", primaryYAML)
	}

	replicaYAML, ok := cm.Data[postgresExporterReplicaQueriesFileName]
	if !ok {
		t.Fatalf("missing key %q", postgresExporterReplicaQueriesFileName)
	}
	for _, want := range []string{
		"wal_replay_lsn:",
		"master: false",
		"pg_last_wal_replay_lsn()",
	} {
		if !strings.Contains(replicaYAML, want) {
			t.Fatalf("replica queries yaml missing %q:\n%s", want, replicaYAML)
		}
	}
	if strings.Contains(replicaYAML, "pg_current_wal_lsn()") {
		t.Fatalf("replica queries yaml must not include pg_current_wal_lsn():\n%s", replicaYAML)
	}
}

func TestPostgresExporterQueriesVolumeMount(t *testing.T) {
	t.Parallel()
	m := postgresExporterQueriesVolumeMount()
	if m.MountPath != postgresExporterQueriesMountPath {
		t.Fatalf("mount path: got %q want %q", m.MountPath, postgresExporterQueriesMountPath)
	}
	if m.SubPath != postgresExporterQueriesFileName {
		t.Fatalf("subPath: got %q want %q", m.SubPath, postgresExporterQueriesFileName)
	}
}

func TestPostgresExporterReplicaQueriesVolumeMount(t *testing.T) {
	t.Parallel()
	m := postgresExporterReplicaQueriesVolumeMount()
	if m.MountPath != postgresExporterQueriesMountPath {
		t.Fatalf("mount path: got %q want %q", m.MountPath, postgresExporterQueriesMountPath)
	}
	if m.SubPath != postgresExporterReplicaQueriesFileName {
		t.Fatalf("subPath: got %q want %q", m.SubPath, postgresExporterReplicaQueriesFileName)
	}
}

func TestPostgresExporterExtendQueryArgs(t *testing.T) {
	t.Parallel()
	args := postgresExporterExtendQueryArgs()
	if len(args) < 2 || args[0] != postgresExporterExtendQueryPathFlag {
		t.Fatalf("args: %+v", args)
	}
	for _, want := range postgresExporterCollectorArgs() {
		if !slices.Contains(args, want) {
			t.Fatalf("args missing %q: %+v", want, args)
		}
	}
}

func TestDBReplicaDeploymentSpec_mountsExporterQueriesVolume(t *testing.T) {
	t.Parallel()
	cfg := DeployConfig{
		Namespace: "cloud9-test-ns",
		Node: DeploymentNode{
			Type: "db",
			Config: map[string]any{
				"readReplicas": 1,
			},
		},
	}
	dep := dbReplicaStatefulSetSpec(cfg, DBResources(), 1, "small")
	var hasQueriesVolume bool
	for _, vol := range dep.Spec.Template.Spec.Volumes {
		if vol.Name == postgresExporterQueriesVolumeName {
			hasQueriesVolume = true
			break
		}
	}
	if !hasQueriesVolume {
		t.Fatal("replica pod must mount postgres exporter queries volume")
	}
	var exporter *corev1.Container
	for i := range dep.Spec.Template.Spec.Containers {
		c := &dep.Spec.Template.Spec.Containers[i]
		if c.Name == "postgres-exporter" {
			exporter = c
			break
		}
	}
	if exporter == nil {
		t.Fatal("expected postgres-exporter sidecar on replica pod")
	}
	wantArgs := postgresExporterExtendQueryArgs()
	if !slices.Equal(exporter.Args, wantArgs) {
		t.Fatalf("exporter args: %+v want %+v", exporter.Args, wantArgs)
	}
	var queriesMount *corev1.VolumeMount
	for i := range exporter.VolumeMounts {
		if exporter.VolumeMounts[i].Name == postgresExporterQueriesVolumeName {
			queriesMount = &exporter.VolumeMounts[i]
			break
		}
	}
	if queriesMount == nil {
		t.Fatal("postgres-exporter must mount queries volume")
	}
	if queriesMount.SubPath != postgresExporterReplicaQueriesFileName {
		t.Fatalf("replica exporter subPath: got %q want %q", queriesMount.SubPath, postgresExporterReplicaQueriesFileName)
	}
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name != "postgres-exporter" && len(c.Args) > 0 {
			t.Fatalf("non-exporter container %q must not have exporter query args: %v", c.Name, c.Args)
		}
	}
}
