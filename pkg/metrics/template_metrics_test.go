package metrics

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestTemplatePrometheusMetricsFromYAML(t *testing.T) {
	t.Parallel()
	cases := []struct {
		template string
		minKeys  int
	}{
		{"social-feed", 15},
		{"social-feed-pull", 15},
		{"ecommerce", 7},
		{"url-shortener", 1},
	}
	for _, tc := range cases {
		tmpl := templates.GetTemplate(tc.template)
		if tmpl == nil {
			t.Fatalf("template %q not found", tc.template)
		}
		if tmpl.PrometheusMetrics == nil {
			t.Fatalf("template %q: PrometheusMetrics nil", tc.template)
		}
		if len(tmpl.PrometheusMetrics.Queries) < tc.minKeys {
			t.Fatalf("template %q: queries=%d want >= %d", tc.template, len(tmpl.PrometheusMetrics.Queries), tc.minKeys)
		}
		for _, q := range tmpl.PrometheusMetrics.Queries {
			if !strings.Contains(q.Expr, templates.NSSelectorPlaceholder) {
				t.Fatalf("template %q key %q: missing placeholder in expr", tc.template, q.Key)
			}
		}
	}
}

func TestSocialFeedPullSharesMetricsProfile(t *testing.T) {
	t.Parallel()
	sf := templates.GetTemplate("social-feed")
	sfp := templates.GetTemplate("social-feed-pull")
	if sf == nil || sfp == nil || sf.PrometheusMetrics == nil || sfp.PrometheusMetrics == nil {
		t.Fatal("expected both templates with metrics")
	}
	if len(sf.PrometheusMetrics.Queries) != len(sfp.PrometheusMetrics.Queries) {
		t.Fatalf("query count mismatch")
	}
	if sf.PrometheusMetrics.Queries[0].Key != sfp.PrometheusMetrics.Queries[0].Key {
		t.Fatalf("expected shared profile")
	}
}

func TestSocialFeedFanoutAvgInYAMLProfile(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("social-feed")
	for _, q := range tmpl.PrometheusMetrics.Queries {
		if q.Key == "worker.fanout_size_avg" {
			if !strings.Contains(q.Expr, "social_worker_fanout_size_sum") {
				t.Fatalf("unexpected expr: %s", q.Expr)
			}
			return
		}
	}
	t.Fatal("worker.fanout_size_avg not in profile")
}

func TestAugmentTemplateMetrics_SocialFeed(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"mq":    {QueueDepth: 42, ConsumeRate: 10, MQSaturationRatio: 0.5, ProduceRate: 15},
			"cache": {RedisOpsPerSec: 100, ActiveConnections: 3, RedisRejectedRate: 0, AvgLatency: 0.001},
		},
		TemplateMetrics: map[string]float64{"worker.consume_rate": 99},
	}
	(&MetricsCollector{}).augmentTemplateMetrics(dm, "social-feed")
	if dm.TemplateMetrics["mq.queue_depth"] != 42 {
		t.Fatalf("mq.queue_depth: got %v", dm.TemplateMetrics["mq.queue_depth"])
	}
	if dm.TemplateMetrics["redis.ops_per_sec"] != 100 {
		t.Fatalf("redis.ops_per_sec: got %v", dm.TemplateMetrics["redis.ops_per_sec"])
	}
}

func TestAugmentTemplateMetrics_EcommerceNoAugments(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"mq": {QueueDepth: 1}}}
	(&MetricsCollector{}).augmentTemplateMetrics(dm, "ecommerce")
	if dm.TemplateMetrics != nil && dm.TemplateMetrics["mq.queue_depth"] != 0 {
		t.Fatalf("ecommerce should not augment mq.queue_depth, got %v", dm.TemplateMetrics["mq.queue_depth"])
	}
}

func TestResolveAugmentPath(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{
		QueueDepth: 12,
		StorageIO: &StorageIO{
			ReadRate:  34,
			BytesUsed: 2048,
		},
	}
	tests := []struct {
		path   string
		want   float64
		wantOK bool
	}{
		{path: "queueDepth", want: 12, wantOK: true},
		{path: "QueueDepth", want: 12, wantOK: true},
		{path: "storageIo.readRate", want: 34, wantOK: true},
		{path: "storageIo.bytesUsed", want: 2048, wantOK: true},
		{path: "storageIo.nope", wantOK: false},
		{path: "doesNotExist", wantOK: false},
	}
	for _, tc := range tests {
		got, ok := resolveAugmentPath(cm, tc.path)
		if ok != tc.wantOK {
			t.Fatalf("%s ok=%v want %v", tc.path, ok, tc.wantOK)
		}
		if ok && got != tc.want {
			t.Fatalf("%s got=%v want %v", tc.path, got, tc.want)
		}
	}
}

func TestResolveAugmentPath_NilPointer(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{}
	if _, ok := resolveAugmentPath(cm, "storageIo.readRate"); ok {
		t.Fatal("expected false for nil storageIo")
	}
}
