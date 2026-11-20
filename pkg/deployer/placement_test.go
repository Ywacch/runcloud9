package deployer

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/placement"
	"github.com/ywacch/cloud9/pkg/templates"
)

type jobWant struct {
	job       string
	nodeID    string
	engine    string
	component string
	shared    bool
	explicit  bool
}

// layoutJobWants is the job-to-node contract for each layout.
// A new engine adds identity checks below, not a full design snapshot.
var layoutJobWants = map[string][]jobWant{
	"social-feed-wq-rmq": {
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
		{job: "timeline", nodeID: "cache-1", engine: "redis", component: "cache"},
		{job: "work_queue", nodeID: "mq-1", engine: "rabbitmq", component: "mq", explicit: true},
	},
	"social-feed-wq-share": {
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
		{job: "timeline", nodeID: "cache-1", engine: "redis", component: "cache", shared: true, explicit: true},
		{job: "work_queue", nodeID: "cache-1", engine: "redis", component: "mq", shared: true, explicit: true},
	},
	"social-feed-wq-dedicated": {
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
		{job: "timeline", nodeID: "cache-tl", engine: "redis", component: "cache", explicit: true},
		{job: "work_queue", nodeID: "cache-wq", engine: "redis", component: "mq", explicit: true},
	},
	"social-feed-tl-pg-wq-rmq": {
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db", shared: true},
		{job: "timeline", nodeID: "db-1", engine: "postgresql", component: "cache", shared: true, explicit: true},
		{job: "work_queue", nodeID: "mq-1", engine: "rabbitmq", component: "mq", explicit: true},
	},
	"social-feed-tl-pg-wq-redis": {
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db", shared: true},
		{job: "timeline", nodeID: "db-1", engine: "postgresql", component: "cache", shared: true, explicit: true},
		{job: "work_queue", nodeID: "cache-wq", engine: "redis", component: "mq", explicit: true},
	},
	"ecommerce-wq-rmq": {
		{job: "hot_cache", nodeID: "cache-1", engine: "redis", component: "cache"},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
		{job: "work_queue", nodeID: "mq-1", engine: "rabbitmq", component: "mq", explicit: true},
	},
	"ecommerce-wq-share": {
		{job: "hot_cache", nodeID: "cache-1", engine: "redis", component: "cache", shared: true, explicit: true},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
		{job: "work_queue", nodeID: "cache-1", engine: "redis", component: "mq", shared: true, explicit: true},
	},
	"ecommerce-wq-dedicated": {
		{job: "hot_cache", nodeID: "cache-1", engine: "redis", component: "cache", explicit: true},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
		{job: "work_queue", nodeID: "cache-wq", engine: "redis", component: "mq", explicit: true},
	},
	"ecommerce-hc-mc-wq-rmq": {
		{job: "hot_cache", nodeID: "cache-1", engine: "memcached", component: "cache", explicit: true},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
		{job: "work_queue", nodeID: "mq-1", engine: "rabbitmq", component: "mq", explicit: true},
	},
	"ecommerce-hc-mc-wq-redis": {
		{job: "hot_cache", nodeID: "cache-1", engine: "memcached", component: "cache", explicit: true},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
		{job: "work_queue", nodeID: "cache-wq", engine: "redis", component: "mq", explicit: true},
	},
	"url-shortener-hc-redis": {
		{job: "hot_cache", nodeID: "cache-1", engine: "redis", component: "cache"},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
	},
	"url-shortener-hc-mc": {
		{job: "hot_cache", nodeID: "cache-1", engine: "memcached", component: "cache"},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
	},
	"url-shortener-proxy": {
		{job: "hot_cache", nodeID: "cache-1", engine: "redis", component: "cache"},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
	},
	"social-feed-pull-hc-redis": {
		{job: "hot_cache", nodeID: "cache-1", engine: "redis", component: "cache", explicit: true},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
	},
	"url-shortener-db-2-replicas": {
		{job: "hot_cache", nodeID: "cache-1", engine: "redis", component: "cache"},
		{job: "main_db", nodeID: "db-1", engine: "postgresql", component: "db"},
	},
}

type memberWant struct {
	layout   string
	nodeID   string
	role     string
	appLabel string
	service  string
	port     string
	replicas int
}

var layoutMemberWants = []memberWant{
	{layout: "ecommerce-wq-dedicated", nodeID: "cache-wq", role: "primary", appLabel: "work-queue", service: "work-queue", port: "6379"},
	{layout: "social-feed-tl-pg-wq-redis", nodeID: "cache-wq", role: "primary", appLabel: "redis", service: "cache", port: "6379"},
	{layout: "url-shortener-hc-mc", nodeID: "cache-1", role: "primary", appLabel: "memcached", service: "cache", port: "11211"},
	{layout: "url-shortener-proxy", nodeID: "cache-1", role: "proxy", appLabel: "cache-proxy", service: "cache", port: "6379"},
	{layout: "url-shortener-db-2-replicas", nodeID: "db-1", role: "replica", appLabel: "postgres-replica", service: "db-replica", port: "5432", replicas: 2},
}

func TestResolvePlacement_LayoutJobs(t *testing.T) {
	t.Parallel()
	if len(layoutJobWants) != len(LayoutFixtureNames) {
		t.Fatalf("job table has %d layouts, names has %d", len(layoutJobWants), len(LayoutFixtureNames))
	}
	for _, name := range LayoutFixtureNames {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := mustResolveLayout(t, name)
			want := layoutJobWants[name]
			if len(got.Jobs) != len(want) {
				t.Fatalf("jobs: got %d want %d", len(got.Jobs), len(want))
			}
			for _, w := range want {
				j, ok := got.JobByID(w.job)
				if !ok {
					t.Fatalf("missing job %q", w.job)
				}
				if j.NodeID != w.nodeID || j.Engine != w.engine || j.Component != w.component || j.Shared != w.shared || j.Explicit != w.explicit {
					t.Fatalf("job %q: got %+v", w.job, j)
				}
				n, ok := got.NodeByID(w.nodeID)
				if !ok {
					t.Fatalf("missing node %q", w.nodeID)
				}
				if !containsString(n.Jobs, w.job) {
					t.Fatalf("node %q jobs %v missing %q", w.nodeID, n.Jobs, w.job)
				}
			}
		})
	}
}

func TestResolvePlacement_LayoutMembers(t *testing.T) {
	t.Parallel()
	for _, w := range layoutMemberWants {
		w := w
		t.Run(w.layout+"/"+w.nodeID+"/"+w.role, func(t *testing.T) {
			t.Parallel()
			got := mustResolveLayout(t, w.layout)
			n, ok := got.NodeByID(w.nodeID)
			if !ok {
				t.Fatalf("missing node %q", w.nodeID)
			}
			var matched []placement.Member
			for _, m := range n.Members {
				if m.Role == w.role && m.AppLabel == w.appLabel && m.Service == w.service && m.Port == w.port {
					matched = append(matched, m)
				}
			}
			wantN := 1
			if w.replicas > 0 {
				wantN = w.replicas
			}
			if len(matched) != wantN {
				t.Fatalf("members %+v: got %d want %d", n.Members, len(matched), wantN)
			}
		})
	}
}

func TestResolvePlacement_Invariants(t *testing.T) {
	t.Parallel()
	for _, name := range LayoutFixtureNames {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			design, ok := LayoutDesign(name)
			if !ok {
				t.Fatal("layout missing")
			}
			tmpl := templates.GetTemplate(design.Template)
			got, err := ResolvePlacement(design, tmpl)
			if err != nil {
				t.Fatalf("ResolvePlacement: %v", err)
			}
			seen := map[string]struct{}{}
			for _, n := range design.Nodes {
				if _, dup := seen[n.ID]; dup {
					t.Fatalf("design repeats node %q", n.ID)
				}
				seen[n.ID] = struct{}{}
			}
			if len(got.Nodes) != len(design.Nodes) {
				t.Fatalf("node count: got %d want %d", len(got.Nodes), len(design.Nodes))
			}
			onNode := map[string]int{}
			for _, j := range got.Jobs {
				if _, ok := seen[j.NodeID]; !ok {
					t.Fatalf("job %q points at missing node %q", j.Job, j.NodeID)
				}
				onNode[j.NodeID]++
			}
			for _, j := range got.Jobs {
				if j.Shared != (onNode[j.NodeID] > 1) {
					t.Fatalf("job %q shared=%v on node with %d jobs", j.Job, j.Shared, onNode[j.NodeID])
				}
			}
			for _, n := range got.Nodes {
				switch n.NodeType {
				case "db", "cache", "mq":
					if len(n.Members) == 0 {
						t.Fatalf("node %q has no members", n.NodeID)
					}
				}
			}
		})
	}
}

func TestResolvePlacement_UnknownEngine(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Template: "url-shortener",
		Nodes: []DeploymentNode{
			{ID: "c1", Type: "cache", Config: map[string]any{"engine": "scylladb"}},
		},
	}
	tmpl := templates.GetTemplate("url-shortener")
	_, err := ResolvePlacement(design, tmpl)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestResolvePlacement_NoJobBindingsMatchesResolveJobBindings(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("social-feed")
	if tmpl == nil {
		t.Fatal("template")
	}
	design := baseSocialFeed(tmpl)
	design.JobBindings = nil
	bindings := ResolveJobBindings(design, tmpl)
	got, err := ResolvePlacement(design, tmpl)
	if err != nil {
		t.Fatalf("ResolvePlacement: %v", err)
	}
	if len(got.Jobs) != len(bindings) {
		t.Fatalf("job count: got %d want %d", len(got.Jobs), len(bindings))
	}
	gotJobs := make(map[string]struct{}, len(got.Jobs))
	for _, j := range got.Jobs {
		gotJobs[j.Job] = struct{}{}
	}
	for _, b := range bindings {
		if _, ok := gotJobs[b.Job]; !ok {
			t.Fatalf("missing job %q", b.Job)
		}
	}
}

func mustResolveLayout(t *testing.T, name string) placement.Topology {
	t.Helper()
	design, ok := LayoutDesign(name)
	if !ok {
		t.Fatalf("layout %q missing", name)
	}
	tmpl := templates.GetTemplate(design.Template)
	if tmpl == nil {
		t.Fatalf("template %q", design.Template)
	}
	got, err := ResolvePlacement(design, tmpl)
	if err != nil {
		t.Fatalf("ResolvePlacement: %v", err)
	}
	return got
}

func containsString(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
