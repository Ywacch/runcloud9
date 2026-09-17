package deployer

import (
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
	yaml, ok := cm.Data[postgresExporterQueriesFileName]
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
		"wal_replay_lsn:",
		"master: false",
		"pg_current_wal_lsn()",
		"pg_last_wal_replay_lsn()",
	} {
		if !strings.Contains(yaml, want) {
			t.Fatalf("queries yaml missing %q:\n%s", want, yaml)
		}
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

func TestPostgresExporterExtendQueryArgs(t *testing.T) {
	t.Parallel()
	args := postgresExporterExtendQueryArgs()
	if len(args) != 1 || args[0] != postgresExporterExtendQueryPathFlag {
		t.Fatalf("args: %+v", args)
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
	if len(exporter.Args) != 1 || exporter.Args[0] != postgresExporterExtendQueryPathFlag {
		t.Fatalf("exporter args: %+v", exporter.Args)
	}
	var hasQueriesMount bool
	for _, m := range exporter.VolumeMounts {
		if m.Name == postgresExporterQueriesVolumeName {
			hasQueriesMount = true
			break
		}
	}
	if !hasQueriesMount {
		t.Fatal("postgres-exporter must mount queries.yaml")
	}
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name != "postgres-exporter" && len(c.Args) > 0 {
			t.Fatalf("non-exporter container %q must not have exporter query args: %v", c.Name, c.Args)
		}
	}
}
