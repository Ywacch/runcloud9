package metrics

import (
	"context"
	"testing"

	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

type fakePodMetricsProvider struct {
	list *metricsv1beta1.PodMetricsList
	err  error
}

func (f *fakePodMetricsProvider) ListPodMetrics(ctx context.Context, namespace string) (*metricsv1beta1.PodMetricsList, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.list, nil
}

func TestQueryContainerResourceCPU_MetricsAPIExcludesExporter(t *testing.T) {
	t.Parallel()

	provider := &fakePodMetricsProvider{list: podMetricsFixture(80, 200<<20)}
	c := NewMetricsCollector("", nil, nil, provider, nil)

	got := c.queryContainerResourceCPU(context.Background(), "ns", "postgres.*", "postgres")
	if got.Source != ResourceSourceMetricsAPI {
		t.Fatalf("source: want %q, got %q", ResourceSourceMetricsAPI, got.Source)
	}
	if got.Usage != 0.08 {
		t.Fatalf("cpu cores: want 0.08, got %v", got.Usage)
	}
	if got.Estimated {
		t.Fatal("expected non-estimated Metrics API CPU")
	}
}

func TestQueryContainerResourceCPU_MetricsAPISkipsNonMatchingPods(t *testing.T) {
	t.Parallel()

	provider := &fakePodMetricsProvider{list: &metricsv1beta1.PodMetricsList{
		Items: []metricsv1beta1.PodMetrics{{
			ObjectMeta: metav1ObjectMeta("loadgen-xyz"),
			Containers: []metricsv1beta1.ContainerMetrics{{Name: "loadgen"}},
		}},
	}}
	c := NewMetricsCollector("", nil, nil, provider, nil)

	got := c.queryContainerResourceCPU(context.Background(), "ns", "postgres.*", "postgres")
	if got.Source != ResourceSourceRequests {
		t.Fatalf("source: want %q when no postgres pod match, got %q", ResourceSourceRequests, got.Source)
	}
}

func TestCompilePodRE(t *testing.T) {
	t.Parallel()
	tests := []struct {
		podRe string
		name  string
		want  bool
	}{
		{podRe: "postgres.*", name: "postgres-569b74fc7d-r5744", want: true},
		{podRe: "postgres.*", name: "postgres-exporter-bad", want: true},
		{podRe: "postgres.*", name: "loadgen-abc", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re, err := compilePodRE(tt.podRe)
			if err != nil {
				t.Fatal(err)
			}
			if got := re.MatchString(tt.name); got != tt.want {
				t.Fatalf("MatchString(%q): want %v, got %v", tt.name, tt.want, got)
			}
		})
	}
}

func TestMetricsAPIPodContainerMemory(t *testing.T) {
	t.Parallel()

	provider := &fakePodMetricsProvider{list: appServerPodMetricsFixture()}
	c := NewMetricsCollector("", nil, nil, provider, nil)

	got := c.metricsAPIPodContainerMemory(context.Background(), "ns", "app-server.*", "app-server")
	if len(got) != 2 {
		t.Fatalf("len: want 2, got %d (%v)", len(got), got)
	}
	if got["app-server-abc"] != 128<<20 {
		t.Fatalf("abc mem: got %d", got["app-server-abc"])
	}
	if got["app-server-def"] != 256<<20 {
		t.Fatalf("def mem: got %d", got["app-server-def"])
	}
}
