package templates

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCloudTemplateRow_toTemplateDef_StorytellingAndWorker(t *testing.T) {
	t.Parallel()

	story := StorytellingSpec{
		Edges: map[string]RoleStorySpec{
			"app-server->cache": {Base: "Remote premium cache edge copy"},
		},
		Roles: map[string]RoleStorySpec{
			"cache": {Base: "Remote cache role"},
		},
	}
	storyRaw, err := json.Marshal(story)
	if err != nil {
		t.Fatal(err)
	}

	row := cloudTemplateRow{
		ID:             "premium-feed",
		Name:           "Premium Feed",
		Description:    "remote",
		Components:     4,
		Bottleneck:     "cache",
		Available:      true,
		Premium:        true,
		Tags:           []string{"premium"},
		Simulatable:    true,
		Deployable:     true,
		ImageKey:       "social-feed",
		WorkerImageKey: "fanout-worker",
		Storytelling:   storyRaw,
	}

	def, err := row.toTemplateDef()
	if err != nil {
		t.Fatalf("toTemplateDef: %v", err)
	}
	if def.WorkerImageKey != "fanout-worker" {
		t.Fatalf("WorkerImageKey: %q", def.WorkerImageKey)
	}
	if def.Storytelling == nil || def.Storytelling.Edges["app-server->cache"].Base == "" {
		t.Fatalf("storytelling edges missing: %+v", def.Storytelling)
	}

	spec := defToSpec(def)
	if !spec.SupportsWorker {
		t.Fatal("SupportsWorker should be true when worker_image_key is set")
	}
	if spec.Storytelling == nil || spec.Storytelling.Edges["app-server->cache"].Base != "Remote premium cache edge copy" {
		t.Fatalf("spec storytelling: %+v", spec.Storytelling)
	}
}

func TestCloudTemplateRow_toTemplateDef_NullStorytelling(t *testing.T) {
	t.Parallel()
	row := cloudTemplateRow{
		ID:           "bare",
		Name:         "Bare",
		Available:    true,
		Premium:      true,
		ImageKey:     "url-shortener",
		Storytelling: json.RawMessage("null"),
	}
	def, err := row.toTemplateDef()
	if err != nil {
		t.Fatal(err)
	}
	if def.Storytelling != nil {
		t.Fatalf("expected nil storytelling, got %+v", def.Storytelling)
	}
	spec := defToSpec(def)
	if spec.SupportsWorker {
		t.Fatal("SupportsWorker should be false without worker_image_key")
	}
}

func TestCloudTemplatesSelectIncludesStorytelling(t *testing.T) {
	t.Parallel()
	if !strings.Contains(cloudTemplatesSelect, "storytelling") {
		t.Fatalf("select missing storytelling: %q", cloudTemplatesSelect)
	}
	if !strings.Contains(cloudTemplatesSelect, "worker_image_key") {
		t.Fatalf("select missing worker_image_key: %q", cloudTemplatesSelect)
	}
}

func TestCloudTemplatesSelectIncludesPremiumConfigColumns(t *testing.T) {
	t.Parallel()
	for _, col := range []string{"loadgen", "prometheus_metrics", "invariant_checkers", "preset", "preset_lite", "introspection"} {
		if !strings.Contains(cloudTemplatesSelect, col) {
			t.Fatalf("select missing %s: %q", col, cloudTemplatesSelect)
		}
	}
}

func TestCloudTemplateRow_toTemplateDef_PremiumConfigJSONB(t *testing.T) {
	t.Parallel()

	loadgenRaw := json.RawMessage(`{
		"defaultRps": 100,
		"endpoints": [
			{"method": "GET", "pathTemplate": "/events/{id}", "weight": 40},
			{"method": "POST", "pathTemplate": "/events/{id}/purchase", "weight": 60}
		],
		"schedules": [
			{
				"id": "drop",
				"label": "Ticket drop",
				"phases": [
					{"durationSeconds": 60, "rpsMultiplier": 1.0, "note": "baseline"},
					{"durationSeconds": 30, "rpsMultiplier": 5.0, "endpointBoost": {"POST /events/{id}/purchase": 10.0}}
				]
			}
		]
	}`)
	metricsRaw := json.RawMessage(`{
		"queries": [
			{"key": "ticket.oversell", "expr": "max(ticket_oversell{<<ns>>})"},
			{"key": "ticket.sold", "expr": "max(ticket_sold{<<ns>>})"}
		]
	}`)
	invariantsRaw := json.RawMessage(`[
		{
			"id": "oversell",
			"kind": "oversell",
			"metric": "ticket.oversell",
			"op": ">",
			"threshold": 0,
			"title": "Oversell detected",
			"narrative": "Sold exceeded capacity"
		}
	]`)

	row := cloudTemplateRow{
		ID:                "ticket-drop",
		Name:              "Ticket Drop",
		Description:       "premium oversell lesson",
		Components:        3,
		Bottleneck:        "oversell",
		Available:         true,
		Premium:           true,
		Tags:              []string{"premium"},
		Simulatable:       true,
		Deployable:        true,
		ImageKey:          "ticket-drop",
		Loadgen:           loadgenRaw,
		PrometheusMetrics: metricsRaw,
		InvariantCheckers: invariantsRaw,
	}

	def, err := row.toTemplateDef()
	if err != nil {
		t.Fatalf("toTemplateDef: %v", err)
	}
	if def.Loadgen == nil || len(def.Loadgen.Schedules) != 1 {
		t.Fatalf("loadgen schedules: %+v", def.Loadgen)
	}
	if def.PrometheusMetrics == nil || len(def.PrometheusMetrics.Queries) != 2 {
		t.Fatalf("prometheus metrics: %+v", def.PrometheusMetrics)
	}
	if len(def.InvariantCheckers) != 1 {
		t.Fatalf("invariant checkers: %+v", def.InvariantCheckers)
	}

	spec := defToSpec(def)
	restore := ReplaceLoadedTemplatesForTest([]TemplateSpec{spec})
	defer restore()

	view, ok := LoadgenConfig("ticket-drop")
	if !ok {
		t.Fatal("LoadgenConfig not found")
	}
	if len(view.Schedules) != 1 || view.Schedules[0].ID != "drop" {
		t.Fatalf("schedules: %+v", view.Schedules)
	}
	if len(view.Endpoints) != 2 {
		t.Fatalf("endpoints: %+v", view.Endpoints)
	}
	if spec.PrometheusMetrics == nil || spec.PrometheusMetrics.Queries[0].Key != "ticket.oversell" {
		t.Fatalf("spec prometheus: %+v", spec.PrometheusMetrics)
	}
	if len(spec.InvariantCheckers) != 1 || spec.InvariantCheckers[0].ID != "oversell" {
		t.Fatalf("spec invariants: %+v", spec.InvariantCheckers)
	}
}

func TestCloudTemplateRow_toTemplateDef_PresetAndIntrospectionJSONB(t *testing.T) {
	t.Parallel()

	presetRaw := json.RawMessage(`{
		"nodes": [
			{"id": "app-1", "type": "compute", "position": [-1, 0.3, 0], "label": "App Server", "config": {"replicas": 1, "instanceSize": "small"}},
			{"id": "db-1", "type": "database", "position": [2, 0.3, 0], "label": "PostgreSQL", "config": {"engine": "postgresql", "storageGB": 10, "maxConnections": 100}}
		],
		"edges": [
			{"id": "e-app-db", "sourceId": "app-1", "targetId": "db-1", "protocol": "tcp"}
		]
	}`)
	introRaw := json.RawMessage(`{
		"db": {"engine": "postgres", "tables": ["events", "purchases"]}
	}`)

	row := cloudTemplateRow{
		ID:            "ticket-drop",
		Name:          "Ticket Drop",
		Available:     true,
		Premium:       true,
		ImageKey:      "ticket-drop",
		Preset:        presetRaw,
		Introspection: introRaw,
	}

	def, err := row.toTemplateDef()
	if err != nil {
		t.Fatalf("toTemplateDef: %v", err)
	}
	if def.Preset == nil || len(def.Preset.Nodes) != 2 {
		t.Fatalf("preset nodes: %+v", def.Preset)
	}
	if def.Introspection == nil || def.Introspection["db"].Engine != "postgres" {
		t.Fatalf("introspection: %+v", def.Introspection)
	}

	spec := defToSpec(def)
	if spec.Preset == nil || len(spec.Preset.Nodes) != 2 {
		t.Fatalf("spec preset: %+v", spec.Preset)
	}
	if spec.Introspection["db"].Tables[0] != "events" {
		t.Fatalf("spec introspection: %+v", spec.Introspection)
	}
}

func TestCloudTemplateRow_toTemplateDef_NullPremiumConfig(t *testing.T) {
	t.Parallel()
	row := cloudTemplateRow{
		ID:                "bare-premium",
		Name:              "Bare",
		Available:         true,
		Premium:           true,
		ImageKey:          "url-shortener",
		Loadgen:           json.RawMessage("null"),
		PrometheusMetrics: json.RawMessage("null"),
		InvariantCheckers: json.RawMessage("null"),
	}
	def, err := row.toTemplateDef()
	if err != nil {
		t.Fatal(err)
	}
	if def.Loadgen != nil || def.PrometheusMetrics != nil || len(def.InvariantCheckers) != 0 {
		t.Fatalf("expected nil premium config, got loadgen=%+v pm=%+v inv=%d",
			def.Loadgen, def.PrometheusMetrics, len(def.InvariantCheckers))
	}
}
