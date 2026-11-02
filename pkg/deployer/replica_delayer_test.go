package deployer

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes"
)

func TestReplicaDelayer_Lookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		engine  string
		wantErr bool
	}{
		{engine: ""},
		{engine: "postgres"},
		{engine: "postgresql"},
		{engine: "POSTGRESQL"},
		{engine: "mysql", wantErr: true},
		{engine: "mongodb", wantErr: true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.engine, func(t *testing.T) {
			t.Parallel()
			_, err := LookupReplicaDelayer(tc.engine)
			if tc.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected: %v", err)
			}
			if tc.wantErr && err != nil && !strings.Contains(err.Error(), tc.engine) {
				t.Fatalf("error %q should name engine", err)
			}
		})
	}
}

func TestPostgresValidate_RemoteApplyBlocksPositiveDelay(t *testing.T) {
	t.Parallel()
	d, err := LookupReplicaDelayer("postgresql")
	if err != nil {
		t.Fatal(err)
	}
	cfg := map[string]any{"replicationMode": "sync", "syncCommitLevel": "remote_apply"}
	err = d.Validate(cfg, []int{0, 1500})
	if err == nil || !strings.Contains(err.Error(), "remote_apply") {
		t.Fatalf("err = %v", err)
	}
}

type recordingDelayer struct {
	hosts []string
	fail  string
}

func (r *recordingDelayer) InitScript([]string, []int) string { return "" }

func (r *recordingDelayer) Validate(map[string]any, []int) error { return nil }

func (r *recordingDelayer) Apply(_ context.Context, _ kubernetes.Interface, _, host string, _ int) error {
	r.hosts = append(r.hosts, host)
	if host == r.fail {
		return fmt.Errorf("apply delay on %s: boom", host)
	}
	return nil
}

func TestReplicaDelayer_ApplyStopsOnFirstError(t *testing.T) {
	t.Parallel()
	d := &recordingDelayer{fail: "postgres-replica-1"}
	err := ApplyReplicaDelays(context.Background(), nil, "ns", d, []string{"postgres-replica-0", "postgres-replica-1", "postgres-replica-2"}, []int{0, 1, 2})
	if err == nil {
		t.Fatal("expected error")
	}
	if len(d.hosts) != 2 {
		t.Fatalf("hosts: %v", d.hosts)
	}
}
