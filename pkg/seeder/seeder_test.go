package seeder

import (
	"strings"
	"testing"
)

func TestPgRestoreCommand_argvForm(t *testing.T) {
	t.Parallel()
	creds := DBCredentials{User: "syscrack", Password: "secret", Database: "syscrack"}
	cmd := pgRestoreCommand(creds)
	joined := strings.Join(cmd, " ")
	for _, want := range []string{"pg_restore", "-U syscrack", "-d syscrack", "--clean", "--if-exists", "/tmp/seed.dump", "PGPASSWORD=secret"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("pgRestoreCommand missing %q: %v", want, cmd)
		}
	}
}

func TestCheckpointCommand_psqlCheckpoint(t *testing.T) {
	t.Parallel()
	creds := DBCredentials{User: "syscrack", Password: "secret", Database: "syscrack"}
	cmd := checkpointCommand(creds)
	joined := strings.Join(cmd, " ")
	for _, want := range []string{"psql", "-U syscrack", "-d syscrack", "ON_ERROR_STOP=1", "CHECKPOINT", "PGPASSWORD=secret"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("checkpointCommand missing %q: %v", want, cmd)
		}
	}
}
