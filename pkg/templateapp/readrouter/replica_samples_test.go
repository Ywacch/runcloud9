package readrouter

import (
	"context"
	"testing"
)

func TestViolationSampleRingCap(t *testing.T) {
	t.Parallel()
	r := &ReadRouter{policy: PolicyRYW, clients: make(map[string]*clientState), maxSize: defaultMaxClients, clientTTL: defaultClientTTL}
	write := lsnToken(100)
	read := lsnToken(50)
	for i := 0; i < replicaViolationSampleCap+3; i++ {
		r.recordViolationSample(context.Background(), "ryw", "client", "postgres-replica-0", "", write, read)
	}
	samples := r.ReplicaSamples()
	if len(samples) != replicaViolationSampleCap {
		t.Fatalf("samples len: got %d want %d", len(samples), replicaViolationSampleCap)
	}
}
