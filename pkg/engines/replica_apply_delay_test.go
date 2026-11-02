package engines

import "testing"

func TestSupportsReplicaApplyDelay(t *testing.T) {
	t.Parallel()
	if !SupportsReplicaApplyDelay("postgresql") {
		t.Fatal("postgresql should support lag dial")
	}
	if SupportsReplicaApplyDelay("mysql") {
		t.Fatal("mysql should not support lag dial yet")
	}
}
