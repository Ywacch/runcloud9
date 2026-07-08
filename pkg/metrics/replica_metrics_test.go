package metrics

import "testing"

func TestMergePodMetricMaps(t *testing.T) {
	t.Parallel()
	names := []string{"app-server-abc", "app-server-def"}
	ready := map[string]bool{"app-server-abc": true, "app-server-def": false}
	rps := map[string]float64{"app-server-abc": 10, "app-server-def": 5}
	p99 := map[string]float64{"app-server-abc": 0.02}
	cpu := map[string]float64{"app-server-def": 0.1}
	errRate := map[string]float64{"app-server-abc": 0.1}

	got := mergePodMetricMaps(names, ready, rps, p99, cpu, errRate, map[string]float64{
		"app-server-abc": 16240,
		"app-server-def": 16105,
	}, map[string]int64{
		"app-server-abc": 134_217_728,
		"app-server-def": 268_435_456,
	})
	if len(got) != 2 {
		t.Fatalf("len = %d want 2", len(got))
	}
	byName := make(map[string]ReplicaMetrics)
	for _, r := range got {
		byName[r.Name] = r
	}
	if byName["app-server-abc"].RequestRate != 10 || !byName["app-server-abc"].Ready {
		t.Fatalf("abc: %+v", byName["app-server-abc"])
	}
	if byName["app-server-abc"].TotalRequests != 16240 {
		t.Fatalf("abc totalRequests: want 16240, got %v", byName["app-server-abc"].TotalRequests)
	}
	if byName["app-server-def"].CPUUsage != 0.1 || byName["app-server-def"].Ready {
		t.Fatalf("def: %+v", byName["app-server-def"])
	}
	if byName["app-server-def"].TotalRequests != 16105 {
		t.Fatalf("def totalRequests: want 16105, got %v", byName["app-server-def"].TotalRequests)
	}
	if byName["app-server-abc"].MemoryBytes != 134_217_728 {
		t.Fatalf("abc memoryBytes: want 134217728, got %d", byName["app-server-abc"].MemoryBytes)
	}
	if byName["app-server-def"].MemoryBytes != 268_435_456 {
		t.Fatalf("def memoryBytes: want 268435456, got %d", byName["app-server-def"].MemoryBytes)
	}
}
