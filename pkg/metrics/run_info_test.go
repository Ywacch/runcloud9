package metrics

import "testing"

func TestCollector_RunInfoOnFrameAndCleanup(t *testing.T) {
	t.Parallel()
	c := NewMetricsCollector("", nil, nil, nil, nil)
	const ns = "ns-run-info"
	c.SetRunInfo(ns, RunInfo{Role: "candidate", ComparisonID: "cmp-uuid-1"})
	dm := &DeploymentMetrics{Namespace: ns}
	c.copyRunInfoOnto(dm)
	if dm.RunRole != "candidate" || dm.ComparisonID != "cmp-uuid-1" {
		t.Fatalf("frame: role=%q comparisonId=%q", dm.RunRole, dm.ComparisonID)
	}
	c.Cleanup(ns)
	dm2 := &DeploymentMetrics{Namespace: ns}
	c.copyRunInfoOnto(dm2)
	if dm2.RunRole != "" || dm2.ComparisonID != "" {
		t.Fatalf("after cleanup: role=%q comparisonId=%q", dm2.RunRole, dm2.ComparisonID)
	}
}
