package deployer

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestSplitQueueCache(t *testing.T) {
	t.Parallel()

	tmpl := mustTemplate(t, "social-feed")

	twoCacheSplit := func() DeploymentDesign {
		d := DeploymentDesign{
			Template: "social-feed",
			Nodes: []DeploymentNode{
				{ID: "app-1", Type: "app-server", Name: "app"},
				{ID: "cache-timeline", Type: "cache", Name: "timeline", Config: map[string]interface{}{"engine": "redis", "evictionPolicy": "allkeys-lru"}},
				{ID: "cache-queue", Type: "cache", Name: "queue", Config: map[string]interface{}{"engine": "redis", "evictionPolicy": "allkeys-lru"}},
				{ID: "db-1", Type: "db", Name: "db"},
				{ID: "worker-1", Type: "worker", Name: "worker"},
			},
			JobBindings: map[string]string{
				"timeline":   "cache-timeline",
				"work_queue": "cache-queue",
			},
		}
		return d
	}

	tests := []struct {
		name      string
		design    DeploymentDesign
		wantOK    bool
		wantQueue string
		wantData  string
	}{
		{
			name:      "two caches timeline and queue",
			design:    twoCacheSplit(),
			wantOK:    true,
			wantQueue: "cache-queue",
			wantData:  "cache-timeline",
		},
		{
			name: "two caches unrelated bindings",
			design: func() DeploymentDesign {
				d := twoCacheSplit()
				d.JobBindings = map[string]string{"timeline": "cache-timeline"}
				return d
			}(),
			wantOK: false,
		},
		{
			name: "single cache share",
			design: func() DeploymentDesign {
				d := deploymentDesignFromTemplatePreset("social-feed", tmpl.Preset)
				d.JobBindings = map[string]string{
					"timeline":   "cache-1",
					"work_queue": "cache-1",
				}
				return d
			}(),
			wantOK: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q, data, ok := splitQueueCache(tc.design)
			if ok != tc.wantOK {
				t.Fatalf("ok: want %v, got %v (q=%q data=%q)", tc.wantOK, ok, q, data)
			}
			if tc.wantOK {
				if q != tc.wantQueue || data != tc.wantData {
					t.Fatalf("ids: want %q/%q, got %q/%q", tc.wantQueue, tc.wantData, q, data)
				}
			}
		})
	}
}

func TestValidateTopology_SplitQueueCaches(t *testing.T) {
	t.Parallel()

	design := DeploymentDesign{
		Template: "social-feed",
		Nodes: []DeploymentNode{
			{ID: "app-1", Type: "app-server", Name: "app"},
			{ID: "c1", Type: "cache", Name: "timeline", Config: map[string]interface{}{"engine": "redis"}},
			{ID: "c2", Type: "cache", Name: "queue", Config: map[string]interface{}{"engine": "redis"}},
			{ID: "db-1", Type: "db", Name: "db", Config: map[string]interface{}{"engine": "postgresql"}},
			{ID: "w1", Type: "worker", Name: "worker"},
		},
		Connections: []DeploymentConn{
			{SourceID: "app-1", TargetID: "db-1"},
			{SourceID: "app-1", TargetID: "c1"},
			{SourceID: "app-1", TargetID: "c2"},
			{SourceID: "w1", TargetID: "db-1"},
			{SourceID: "w1", TargetID: "c1"},
			{SourceID: "w1", TargetID: "c2"},
		},
		JobBindings: map[string]string{
			"timeline":   "c1",
			"work_queue": "c2",
		},
	}
	if err := ValidateTopology(design); err != nil {
		t.Fatalf("ValidateTopology: %v", err)
	}
}

func TestValidateTopology_TwoCachesWithoutSplitStillFails(t *testing.T) {
	t.Parallel()

	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a1", Type: "app-server", Name: "App"},
			{ID: "c1", Type: "cache", Name: "Cache 1"},
			{ID: "c2", Type: "cache", Name: "Cache 2"},
			{ID: "d1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a1", TargetID: "d1"},
			{SourceID: "a1", TargetID: "c1"},
			{SourceID: "a1", TargetID: "c2"},
		},
	}
	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "only one cache") {
		t.Fatalf("got %q", err.Error())
	}
}

func TestJobEnv_SplitQueueRedis(t *testing.T) {
	t.Parallel()

	design := DeploymentDesign{
		Template: "social-feed",
		Nodes: []DeploymentNode{
			{ID: "app-1", Type: "app-server", Name: "app"},
			{ID: "cache-timeline", Type: "cache", Name: "timeline", Config: map[string]interface{}{"engine": "redis"}},
			{ID: "cache-queue", Type: "cache", Name: "queue", Config: map[string]interface{}{"engine": "redis"}},
			{ID: "db-1", Type: "db", Name: "db", Config: map[string]interface{}{"engine": "postgresql"}},
			{ID: "worker-1", Type: "worker", Name: "worker"},
		},
		Connections: []DeploymentConn{
			{SourceID: "app-1", TargetID: "db-1"},
			{SourceID: "app-1", TargetID: "cache-timeline"},
			{SourceID: "app-1", TargetID: "cache-queue"},
			{SourceID: "worker-1", TargetID: "db-1"},
			{SourceID: "worker-1", TargetID: "cache-timeline"},
			{SourceID: "worker-1", TargetID: "cache-queue"},
		},
		JobBindings: map[string]string{
			"timeline":   "cache-timeline",
			"work_queue": "cache-queue",
		},
	}
	tmpl := mustTemplate(t, "social-feed")
	bindings := ResolveJobBindings(design, tmpl)
	app := nodeByID(design, "app-1")
	env := jobEnv(*app, design, bindings)
	if env["TIMELINE_HOST"] != "cache" {
		t.Fatalf("timeline host: %q", env["TIMELINE_HOST"])
	}
	if env["WORK_QUEUE_HOST"] != "work-queue" {
		t.Fatalf("work queue host: %q", env["WORK_QUEUE_HOST"])
	}
}

func TestRedisPolicyForNode(t *testing.T) {
	t.Parallel()

	timelineOnly := DeploymentNode{
		ID:     "c1",
		Type:   "cache",
		Config: map[string]interface{}{"evictionPolicy": "allkeys-lru"},
	}
	queueOnly := DeploymentNode{ID: "c2", Type: "cache", Config: map[string]interface{}{"evictionPolicy": "allkeys-lru"}}

	shareDesign := DeploymentDesign{
		Template: "social-feed",
		JobBindings: map[string]string{
			"timeline":   "c1",
			"work_queue": "c1",
		},
	}
	splitDesign := DeploymentDesign{
		Template: "social-feed",
		JobBindings: map[string]string{
			"timeline":   "c1",
			"work_queue": "c2",
		},
	}
	queueOnlyDesign := DeploymentDesign{
		Template: "social-feed",
		JobBindings: map[string]string{
			"timeline":   "db-1",
			"work_queue": "c2",
		},
	}

	if got := redisPolicyForNode(timelineOnly, shareDesign); got != "allkeys-lru" {
		t.Fatalf("share policy: %q", got)
	}
	if got := redisPolicyForNode(queueOnly, splitDesign); got != "noeviction" {
		t.Fatalf("split queue policy: %q", got)
	}
	if got := redisPolicyForNode(queueOnly, queueOnlyDesign); got != "noeviction" {
		t.Fatalf("queue-only policy: %q", got)
	}
}

func TestRedisServerArgsForNode_SplitQueue(t *testing.T) {
	t.Parallel()

	node := DeploymentNode{
		ID:     "cache-queue",
		Type:   "cache",
		Config: map[string]interface{}{"evictionPolicy": "allkeys-lru"},
	}
	design := DeploymentDesign{
		Template:    "social-feed",
		JobBindings: map[string]string{"timeline": "c1", "work_queue": "cache-queue"},
	}
	args := redisServerArgsForNode(node, design, corev1.ResourceRequirements{})
	if !containsArg(args, "--maxmemory-policy", "noeviction") {
		t.Fatalf("args: %v", args)
	}
}

func containsArg(args []string, flag, want string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == want {
			return true
		}
	}
	return false
}

func mustTemplate(t *testing.T, id string) *templates.TemplateSpec {
	t.Helper()
	spec := templates.GetTemplate(id)
	if spec == nil {
		t.Fatalf("template %q missing", id)
	}
	return spec
}
