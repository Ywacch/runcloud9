package deployer

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPostgresPrimaryCommand_omitsSyncGUCsForSemiSyncAndSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  map[string]any
	}{
		{
			name: "semi-sync",
			cfg: map[string]any{
				"replicationMode": "semi-sync",
				"readReplicas":    2,
				"syncCommitLevel": "remote_apply",
			},
		},
		{
			name: "sync",
			cfg: map[string]any{
				"replicationMode": "sync",
				"readReplicas":    1,
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := strings.Join(postgresPrimaryCommand(tc.cfg), " ")
			for _, forbidden := range []string{"synchronous_standby_names", "synchronous_commit"} {
				if strings.Contains(cmd, forbidden) {
					t.Fatalf("primary command must not include %q at boot: %q", forbidden, cmd)
				}
			}
			if !strings.Contains(cmd, "wal_level=replica") {
				t.Fatalf("expected wal_level=replica in command: %q", cmd)
			}
		})
	}
}

func TestPostgresSyncGUCsFromConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cfg    map[string]any
		want   postgresSyncGUCs
		wantOK bool
	}{
		{
			name: "semi-sync remote_apply",
			cfg: map[string]any{
				"replicationMode": "semi-sync",
				"readReplicas":    2,
				"syncCommitLevel": "remote_apply",
			},
			want: postgresSyncGUCs{
				StandbyNames: "ANY 1 (*)",
				CommitLevel:  "remote_apply",
			},
			wantOK: true,
		},
		{
			name: "sync two replicas",
			cfg: map[string]any{
				"replicationMode": "sync",
				"readReplicas":    2,
				"syncCommitLevel": "remote_write",
			},
			want: postgresSyncGUCs{
				StandbyNames: "FIRST 2 (*)",
				CommitLevel:  "remote_write",
			},
			wantOK: true,
		},
		{
			name: "async",
			cfg: map[string]any{
				"replicationMode": "async",
				"readReplicas":    2,
			},
			wantOK: false,
		},
		{
			name: "sync without replicas",
			cfg: map[string]any{
				"replicationMode": "sync",
				"readReplicas":    0,
			},
			wantOK: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := postgresSyncGUCsFromConfig(tc.cfg)
			if ok != tc.wantOK {
				t.Fatalf("ok: got %v want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if got != tc.want {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

func TestEnableSynchronousReplicationStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		gucs postgresSyncGUCs
		want []string
	}{
		{
			name: "semi-sync",
			gucs: postgresSyncGUCs{StandbyNames: "ANY 1 (*)", CommitLevel: "on"},
			want: []string{
				"synchronous_standby_names = 'ANY 1 (*)'",
				"synchronous_commit = 'on'",
				"pg_reload_conf()",
			},
		},
		{
			name: "sync remote_apply",
			gucs: postgresSyncGUCs{StandbyNames: "FIRST 2 (*)", CommitLevel: "remote_apply"},
			want: []string{
				"synchronous_standby_names = 'FIRST 2 (*)'",
				"synchronous_commit = 'remote_apply'",
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := enableSynchronousReplicationStatements(tc.gucs)
			if len(got) != 3 {
				t.Fatalf("expected 3 statements, got %d: %v", len(got), got)
			}
			joined := strings.Join(got, "; ")
			for _, fragment := range tc.want {
				if !strings.Contains(joined, fragment) {
					t.Fatalf("SQL missing %q: %q", fragment, joined)
				}
			}
		})
	}
}

func TestParseReplicationCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stdout  string
		want    int
		wantErr bool
	}{
		{name: "one", stdout: "1\n", want: 1},
		{name: "spaces", stdout: "  2  ", want: 2},
		{name: "empty", stdout: "", want: 0},
		{name: "bad", stdout: "x", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseReplicationCount(tc.stdout)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestGucsMatchReadBack(t *testing.T) {
	t.Parallel()

	want := postgresSyncGUCs{StandbyNames: "ANY 1 (*)", CommitLevel: "remote_apply"}
	if !gucsMatchReadBack(want, "ANY 1 (*)", "remote_apply") {
		t.Fatal("expected match")
	}
	if !gucsMatchReadBack(want, "'ANY 1 (*)'", "'remote_apply'") {
		t.Fatal("expected quoted match")
	}
	if gucsMatchReadBack(want, "ANY 1 (*)", "on") {
		t.Fatal("expected commit mismatch")
	}
}

type fakePostgresAdmin struct {
	counts      []int
	countIdx    int
	showStandby string
	showCommit  string
	execCalls   []string
}

func (f *fakePostgresAdmin) execSQL(_ context.Context, query string) (string, error) {
	f.execCalls = append(f.execCalls, query)
	switch {
	case query == sqlCountStreamingReplicas || query == sqlCountSyncQuorumReplicas:
		if f.countIdx >= len(f.counts) {
			return "0", nil
		}
		out := f.counts[f.countIdx]
		f.countIdx++
		return strconv.Itoa(out), nil
	case query == "SHOW synchronous_standby_names":
		return f.showStandby, nil
	case query == "SHOW synchronous_commit":
		return f.showCommit, nil
	case strings.Contains(query, "ALTER SYSTEM"):
		return "", nil
	case query == "SELECT pg_reload_conf()":
		return "", nil
	default:
		return "", errors.New("unexpected query")
	}
}

func TestEnablePostgresSynchronousReplication_success(t *testing.T) {
	t.Parallel()

	admin := &fakePostgresAdmin{
		counts:      []int{0, 1, 1},
		showStandby: "ANY 1 (*)",
		showCommit:  "remote_apply",
	}
	cfg := map[string]any{
		"replicationMode": "semi-sync",
		"readReplicas":    1,
		"syncCommitLevel": "remote_apply",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := enablePostgresSynchronousReplication(ctx, admin, cfg, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	alterCount := 0
	for _, call := range admin.execCalls {
		if strings.Contains(call, "ALTER SYSTEM") {
			alterCount++
		}
	}
	if alterCount != 2 {
		t.Fatalf("expected 2 ALTER SYSTEM calls, got %d: %v", alterCount, admin.execCalls)
	}
}

func TestEnablePostgresSynchronousReplication_readBackMismatch(t *testing.T) {
	t.Parallel()

	admin := &fakePostgresAdmin{
		counts:      []int{1, 1},
		showStandby: "ANY 1 (*)",
		showCommit:  "on",
	}
	cfg := map[string]any{
		"replicationMode": "semi-sync",
		"readReplicas":    1,
		"syncCommitLevel": "remote_apply",
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := enablePostgresSynchronousReplication(ctx, admin, cfg, 1)
	if err == nil {
		t.Fatal("expected read-back mismatch error")
	}
	if !strings.Contains(err.Error(), "read-back mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForReplicationCount_timeout(t *testing.T) {
	t.Parallel()

	admin := &fakePostgresAdmin{counts: []int{0, 0, 0}}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := waitForReplicationCount(ctx, admin, sqlCountStreamingReplicas, 1)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}
