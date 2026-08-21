package deployer

import (
	"strings"
	"testing"
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
	for _, want := range []string{"replay_lag", "application_name", "replay_lag_seconds", "master: true"} {
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

func TestDBReplicaDeploymentSpec_noExporterQueriesVolume(t *testing.T) {
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
	dep := dbReplicaDeploymentSpec(cfg, DBResources(), 1)
	for _, vol := range dep.Spec.Template.Spec.Volumes {
		if vol.Name == postgresExporterQueriesVolumeName {
			t.Fatalf("replica pod must not mount postgres exporter queries volume")
		}
	}
	for _, c := range dep.Spec.Template.Spec.Containers {
		if len(c.Args) > 0 {
			t.Fatalf("replica container %q must not have exporter query args: %v", c.Name, c.Args)
		}
	}
}
