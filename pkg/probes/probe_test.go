package probes

import (
	"strings"
	"testing"
)

func TestProbeCreateTableSQL_containsTableName(t *testing.T) {
	t.Parallel()
	if !strings.Contains(probeCreateTableSQL, ProbeTableName) {
		t.Fatalf("DDL missing table %q: %s", ProbeTableName, probeCreateTableSQL)
	}
}

func TestMonotonicViolation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		lastSeen int64
		current  int64
		want     bool
	}{
		{name: "equal ok", lastSeen: 5, current: 5, want: false},
		{name: "advance ok", lastSeen: 5, current: 6, want: false},
		{name: "regression", lastSeen: 5, current: 4, want: true},
		{name: "zero baseline", lastSeen: 0, current: 1, want: false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := MonotonicViolation(tc.lastSeen, tc.current); got != tc.want {
				t.Fatalf("MonotonicViolation(%d, %d) = %v, want %v", tc.lastSeen, tc.current, got, tc.want)
			}
		})
	}
}

func TestStalenessVersions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		written int64
		read    int64
		want    int64
	}{
		{name: "caught up", written: 10, read: 10, want: 0},
		{name: "ahead", written: 10, read: 12, want: 0},
		{name: "one behind", written: 10, read: 9, want: 1},
		{name: "many behind", written: 100, read: 42, want: 58},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := StalenessVersions(tc.written, tc.read); got != tc.want {
				t.Fatalf("StalenessVersions(%d, %d) = %d, want %d", tc.written, tc.read, got, tc.want)
			}
		})
	}
}

func TestPostgresProbe_implementsConsistencyProbe(t *testing.T) {
	t.Parallel()
	var _ ConsistencyProbe = (*PostgresProbe)(nil)
}
