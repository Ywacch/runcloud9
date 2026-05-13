package metrics

import "testing"

func TestInterpolateNarrativePlaceholders(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		TemplateMetrics: map[string]float64{
			"worker.fanout_size_p99": 5000,
			"mq.produce_rate":        12.5,
		},
	}
	s := "fanout {worker.fanout_size_p99} rate {mq.produce_rate} missing {unknown.key}"
	got := interpolateNarrativePlaceholders(s, dm)
	want := "fanout 5000 rate 12.5 missing {unknown.key}"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
