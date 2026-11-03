package templates

import (
	"slices"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/engines"
)

func TestTemplates_JobsDeclared(t *testing.T) {
	t.Parallel()

	type wantJob struct {
		id       string
		required bool
	}

	tests := []struct {
		templateID string
		want       []wantJob
	}{
		{
			templateID: "social-feed",
			want: []wantJob{
				{id: "main_db", required: true},
				{id: "timeline", required: true},
				{id: "work_queue", required: false},
			},
		},
		{
			templateID: "social-feed-pull",
			want: []wantJob{
				{id: "main_db", required: true},
				{id: "hot_cache", required: false},
			},
		},
		{
			templateID: "url-shortener",
			want: []wantJob{
				{id: "main_db", required: true},
				{id: "hot_cache", required: false},
			},
		},
		{
			templateID: "ecommerce",
			want: []wantJob{
				{id: "main_db", required: true},
				{id: "hot_cache", required: false},
				{id: "work_queue", required: false},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.templateID, func(t *testing.T) {
			t.Parallel()
			spec := GetTemplate(tt.templateID)
			if spec == nil {
				t.Fatalf("template %q not found", tt.templateID)
			}
			if len(spec.Jobs) != len(tt.want) {
				t.Fatalf("jobs len: want %d, got %d (%+v)", len(tt.want), len(spec.Jobs), spec.Jobs)
			}
			for i, w := range tt.want {
				got := spec.Jobs[i]
				if got.ID != w.id {
					t.Fatalf("job[%d] id: want %q, got %q", i, w.id, got.ID)
				}
				if got.Required != w.required {
					t.Fatalf("job %q required: want %v, got %v", w.id, w.required, got.Required)
				}
				if len(got.AllowedEngines) == 0 {
					t.Fatalf("job %q: missing allowedEngines", w.id)
				}
			}
		})
	}
}

func TestHotCacheAllowsMemcached(t *testing.T) {
	t.Parallel()

	tests := []string{"url-shortener", "ecommerce"}
	for _, id := range tests {
		t.Run(id, func(t *testing.T) {
			t.Parallel()
			spec := GetTemplate(id)
			if spec == nil {
				t.Fatalf("template %q missing", id)
			}
			var allowed []string
			for _, job := range spec.Jobs {
				if job.ID == "hot_cache" {
					allowed = job.AllowedEngines
					break
				}
			}
			if !slices.Contains(allowed, "redis") || !slices.Contains(allowed, "memcached") {
				t.Fatalf("hot_cache allowedEngines: %v", allowed)
			}
		})
	}
}

func TestWorkQueueAllowsRabbitMQAndRedis(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"social-feed", "ecommerce"} {
		t.Run(id, func(t *testing.T) {
			t.Parallel()
			spec := GetTemplate(id)
			if spec == nil {
				t.Fatalf("template %q missing", id)
			}
			var allowed []string
			for _, job := range spec.Jobs {
				if job.ID == "work_queue" {
					allowed = job.AllowedEngines
					break
				}
			}
			if !slices.Contains(allowed, "rabbitmq") || !slices.Contains(allowed, "redis") {
				t.Fatalf("work_queue allowedEngines: %v", allowed)
			}
		})
	}
}

func TestTemplates_UnknownJobRejected(t *testing.T) {
	t.Parallel()

	def := &templateDef{
		ID: "bad-jobs",
		Jobs: []JobRequirement{{
			ID:              "not_a_real_job",
			Required:        true,
			AllowedEngines:  []string{"postgresql"},
			DefaultNodeType: "db",
		}},
	}
	_, err := resolveJobRequirements(def, engines.Default())
	if err == nil || !strings.Contains(err.Error(), "unknown job id") {
		t.Fatalf("want unknown job error, got %v", err)
	}
}

func TestTemplates_WorkerTimelineStoreAlias(t *testing.T) {
	t.Parallel()

	def := &templateDef{
		ID:                  "alias-test",
		WorkerTimelineStore: "redis",
		Jobs: []JobRequirement{{
			ID:              "main_db",
			Required:        true,
			AllowedEngines:  []string{"postgresql"},
			DefaultNodeType: "db",
		}},
	}
	jobs, err := resolveJobRequirements(def, engines.Default())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	var timeline *JobRequirement
	for i := range jobs {
		if jobs[i].ID == "timeline" {
			timeline = &jobs[i]
			break
		}
	}
	if timeline == nil {
		t.Fatalf("want synthesized timeline job, got %+v", jobs)
	}
	if !timeline.Required || timeline.DefaultNodeType != "cache" {
		t.Fatalf("timeline job: %+v", timeline)
	}
	if len(timeline.AllowedEngines) != 1 || timeline.AllowedEngines[0] != "redis" {
		t.Fatalf("timeline allowedEngines: %+v", timeline.AllowedEngines)
	}
}

func TestTemplates_UnknownEngineRejected(t *testing.T) {
	t.Parallel()

	def := &templateDef{
		ID: "bad-engine",
		Jobs: []JobRequirement{{
			ID:              "main_db",
			Required:        true,
			AllowedEngines:  []string{"cassandra"},
			DefaultNodeType: "db",
		}},
	}
	_, err := resolveJobRequirements(def, engines.Default())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "unknown engine id") && !strings.Contains(err.Error(), "no catalog pair") {
		t.Fatalf("want engine validation error, got %v", err)
	}
}
