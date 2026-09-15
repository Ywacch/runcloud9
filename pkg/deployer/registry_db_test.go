package deployer

import (
	"strings"
	"testing"
)

func TestComponentRegistryDB_replicationReady(t *testing.T) {
	t.Parallel()

	spec, ok := componentRegistry["db"]
	if !ok {
		t.Fatal("missing db component spec")
	}
	if spec.DefaultEnv["POSTGRES_HOST_AUTH_METHOD"] != "trust" {
		t.Fatalf("POSTGRES_HOST_AUTH_METHOD: got %q want trust", spec.DefaultEnv["POSTGRES_HOST_AUTH_METHOD"])
	}
	cmd := strings.Join(spec.Command, " ")
	for _, want := range []string{
		"max_replication_slots=10",
		"wal_level=replica",
		"max_wal_senders=10",
		"wal_keep_size=64MB",
		"hot_standby=on",
	} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("db command missing %q: %q", want, cmd)
		}
	}
}
