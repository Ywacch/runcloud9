package hub

import (
	"strings"
	"testing"
	"time"
)

func TestContainerUsage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	snap := newSnapshot(now)
	snap.podUsage = metricsAPIState{
		At: now,
		ByPod: map[string]map[string]map[string]containerUsage{
			"ns-a": {
				"app-server-1": {"app-server": {CPUMilli: 100, MemBytes: 200}},
				"app-server-2": {"app-server": {CPUMilli: 50, MemBytes: 80}},
				"worker-1":     {"worker": {CPUMilli: 10, MemBytes: 20}},
			},
		},
	}

	include := func(name string) bool { return strings.HasPrefix(name, "app-server") }
	cpu, mem, found, sampled := snap.ContainerUsage("ns-a", "app-server", include, nil)
	if !sampled || !found || cpu != 150 || mem != 280 {
		t.Fatalf("got cpu=%d mem=%d found=%v sampled=%v", cpu, mem, found, sampled)
	}

	_, _, found, sampled = snap.ContainerUsage("ns-a", "missing", include, nil)
	if !sampled || found {
		t.Fatalf("missing container: found=%v sampled=%v", found, sampled)
	}

	empty := newSnapshot(now)
	_, _, found, sampled = empty.ContainerUsage("ns-a", "app-server", nil, nil)
	if sampled || found {
		t.Fatalf("unsampled: found=%v sampled=%v", found, sampled)
	}
}
