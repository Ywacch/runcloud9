package metrics

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestNarrate_AllRulesNonEmpty(t *testing.T) {
	t.Parallel()
	topo := TopologyFlags{
		HasCache:      true,
		HasMQ:         true,
		HasWorker:     true,
		HasLB:         true,
		CacheStrategy: "cache-aside",
		Template:      "url-shortener",
	}
	dm := deploymentForNarrativeTests("url-shortener")
	tmpl := templates.GetTemplate("url-shortener")

	rules := []string{
		ruleCacheHitLow,
		ruleErrorRate,
		ruleDBLatency,
		ruleQueueDepth,
		ruleDBConnPressure,
		ruleMQBackpressure,
		cascadeRuleCacheDB,
		cascadeRuleQueueWorker,
		cascadeRuleDBPoolErrors,
		cascadeRuleLatencyMiss,
		cascadeRuleMQBackpressure,
	}
	for _, rule := range rules {
		rule := rule
		t.Run(rule, func(t *testing.T) {
			t.Parallel()
			out := Narrate(rule, dm, topo, tmpl)
			if out.Narrative == "" {
				t.Fatalf("Narrative empty for %s", rule)
			}
			if out.Suggestion == "" {
				t.Fatalf("Suggestion empty for %s", rule)
			}
		})
	}
}

func TestNarrate_RelatedComponents(t *testing.T) {
	t.Parallel()
	topo := TopologyFlags{HasCache: true, HasMQ: true, HasWorker: true, Template: "url-shortener"}
	dm := deploymentForNarrativeTests("url-shortener")
	tmpl := templates.GetTemplate("url-shortener")

	tests := []struct {
		rule     string
		required []string
	}{
		{ruleCacheHitLow, []string{"cache", "db"}},
		{ruleQueueDepth, []string{"mq", "worker"}},
		{ruleMQBackpressure, []string{"mq", "worker", "cache"}},
		{cascadeRuleCacheDB, []string{"cache", "db"}},
		{cascadeRuleLatencyMiss, []string{"app-server", "cache", "db"}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.rule, func(t *testing.T) {
			t.Parallel()
			out := Narrate(tt.rule, dm, topo, tmpl)
			for _, comp := range tt.required {
				if !containsString(out.Related, comp) {
					t.Fatalf("Related %#v missing %q", out.Related, comp)
				}
			}
		})
	}
}

func TestNarrate_SocialFeedMQBackpressureIncludesFanout(t *testing.T) {
	t.Parallel()
	topo := TopologyFlags{
		HasCache:      true,
		HasMQ:         true,
		HasWorker:     true,
		Template:      "social-feed",
		CacheStrategy: "timeline-store",
	}
	dm := deploymentForNarrativeTests("social-feed")
	dm.TemplateMetrics["worker.fanout_size_p99"] = 5000
	dm.TemplateMetrics["mq.produce_rate"] = 12
	dm.TemplateMetrics["worker.redis_write_rate"] = 48000

	tmpl := templates.GetTemplate("social-feed")
	out := Narrate(ruleMQBackpressure, dm, topo, tmpl)
	if !strings.Contains(out.Narrative, "5000") {
		t.Fatalf("expected fanout in narrative: %q", out.Narrative)
	}
	if !strings.Contains(out.Narrative, "48000") || !strings.Contains(out.Narrative, "12") {
		t.Fatalf("expected produce/write rates in narrative: %q", out.Narrative)
	}
}

func TestNarrate_UrlShortenerNoSocialFeedJargon(t *testing.T) {
	t.Parallel()
	topo := TopologyFlags{HasCache: true, Template: "url-shortener", CacheStrategy: "cache-aside"}
	dm := deploymentForNarrativeTests("url-shortener")
	tmpl := templates.GetTemplate("url-shortener")
	out := Narrate(ruleMQBackpressure, dm, topo, tmpl)
	if strings.Contains(strings.ToLower(out.Narrative), "fan-out") {
		t.Fatalf("unexpected fan-out wording for url-shortener: %q", out.Narrative)
	}
}

func containsString(slice []string, s string) bool {
	for _, x := range slice {
		if x == s {
			return true
		}
	}
	return false
}

func deploymentForNarrativeTests(template string) *DeploymentMetrics {
	dm := &DeploymentMetrics{
		TemplateMetrics: make(map[string]float64),
		Components: map[string]ComponentMetrics{
			"app-server": {
				Component:   "app-server",
				RequestRate: 100,
				ErrorRate:   2.5,
				P99Latency:  0.9,
			},
			"cache": {
				Component:         "cache",
				CacheHitsLast9m:   20,
				CacheMissesLast9m: 80,
				RedisOpsPerSec:    500,
				RedisRejectedRate: 0.1,
				CacheStrategy:     "cache-aside",
			},
			"db": {
				Component:           "db",
				DbQueryRate:         200,
				DbQueryLatency:      0.75,
				DbActiveConnections: 85,
				MaxConnections:      100,
			},
			"mq": {
				Component:         "mq",
				QueueDepth:        150,
				ProduceRate:       80,
				ConsumeRate:       20,
				MQSaturationRatio: 3,
				CPUUsage:          0.2,
			},
			"worker": {
				Component:   "worker",
				CPUUsage:    0.45,
				RequestRate: 18,
			},
			"lb": {
				Component:         "lb",
				CPUUsage:          0.05,
				ActiveConnections: 50,
				RequestRate:       100,
			},
		},
	}
	if template == "social-feed" {
		dm.Components["cache"] = ComponentMetrics{
			Component:         "cache",
			CacheHitsLast9m:   15,
			CacheMissesLast9m: 85,
			RedisOpsPerSec:    12000,
			CacheStrategy:     "timeline-store",
		}
	}
	return dm
}

func TestNarratePodOOM_withContext(t *testing.T) {
	t.Parallel()
	ctx := &CrashContext{MemPercent: 96.5, TerminationReason: "OOMKilled"}
	out := narratePodOOM(ctx)
	if out.Narrative == "" || out.Suggestion == "" {
		t.Fatalf("narrative: %+v", out)
	}
	if !strings.Contains(out.Narrative, "OOMKilled") {
		t.Fatalf("want OOMKilled mention: %q", out.Narrative)
	}
}

func TestNarratePodRestart_withContext(t *testing.T) {
	t.Parallel()
	ctx := &CrashContext{
		WaitingReason:     "CrashLoopBackOff",
		RedisRejectedRate: 120,
		ErrorRate:         2.5,
	}
	out := narratePodRestart(ctx)
	if out.Narrative == "" || out.Suggestion == "" {
		t.Fatalf("narrative: %+v", out)
	}
	if !strings.Contains(out.Narrative, "CrashLoopBackOff") {
		t.Fatalf("want crash loop mention: %q", out.Narrative)
	}
}
