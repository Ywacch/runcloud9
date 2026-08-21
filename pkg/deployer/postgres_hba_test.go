package deployer

import (
	"strings"
	"testing"
)

func TestPostgresReplicationHBAConfigMap_usesPostgresUser(t *testing.T) {
	t.Parallel()

	cm := postgresReplicationHBAConfigMap("cloud9-test-ns")
	if cm.Name != postgresReplicationHBAConfigMapName {
		t.Fatalf("configmap name: got %q want %q", cm.Name, postgresReplicationHBAConfigMapName)
	}
	script, ok := cm.Data[postgresReplicationHBAScriptName]
	if !ok {
		t.Fatalf("missing script key %q", postgresReplicationHBAScriptName)
	}
	for _, want := range []string{
		"host replication ${POSTGRES_USER} 0.0.0.0/0 trust",
		"host replication ${POSTGRES_USER} ::/0 trust",
		"$PGDATA/pg_hba.conf",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "host all") {
		t.Fatal("script should not rewrite host all; docker-entrypoint already sets that")
	}
	if strings.Contains(script, "syscrack") {
		t.Fatal("script should use POSTGRES_USER, not a hardcoded role")
	}
}

func TestPostgresReplicationHBAEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  DeployConfig
		want bool
	}{
		{
			name: "postgres db",
			cfg:  DeployConfig{Node: DeploymentNode{Type: "db", Config: map[string]any{"engine": "postgresql"}}},
			want: true,
		},
		{
			name: "default engine is postgres",
			cfg:  DeployConfig{Node: DeploymentNode{Type: "db"}},
			want: true,
		},
		{
			name: "mysql db skipped",
			cfg:  DeployConfig{Node: DeploymentNode{Type: "db", Config: map[string]any{"engine": "mysql"}}},
			want: false,
		},
		{
			name: "mq skipped",
			cfg:  DeployConfig{Node: DeploymentNode{Type: "mq"}},
			want: false,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := postgresReplicationHBAEnabled(tc.cfg); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestPostgresReplicationHBAVolumeMount_initdbPath(t *testing.T) {
	t.Parallel()

	m := postgresReplicationHBAVolumeMount()
	if m.MountPath != postgresReplicationHBAMountPath {
		t.Fatalf("mount path: got %q want %q", m.MountPath, postgresReplicationHBAMountPath)
	}
	if m.SubPath != postgresReplicationHBAScriptName {
		t.Fatalf("subPath: got %q want %q", m.SubPath, postgresReplicationHBAScriptName)
	}
	if !m.ReadOnly {
		t.Fatal("initdb script mount should be read-only")
	}
	v := postgresReplicationHBAVolume()
	if v.ConfigMap == nil || v.ConfigMap.Name != postgresReplicationHBAConfigMapName {
		t.Fatalf("volume configmap: %+v", v.ConfigMap)
	}
}
