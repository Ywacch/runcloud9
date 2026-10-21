package api

import (
	"testing"
)

func TestFormatReplicaLagDescription(t *testing.T) {
	t.Parallel()
	got := formatReplicaLagDescription(
		[]string{"postgres-replica-0", "postgres-replica-1"},
		[]int{0, 1500},
	)
	want := "postgres-replica-0=0ms, postgres-replica-1=1500ms"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
