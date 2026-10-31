package deployer

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestResolveJobBindings(t *testing.T) {
	t.Parallel()

	type wantBinding struct {
		job      string
		nodeID   string
		engine   string
		explicit bool
	}

	tests := []struct {
		name     string
		template string
		preset   func(*templates.TemplateSpec) *templates.TemplatePreset
		mutate   func(*DeploymentDesign)
		want     []wantBinding
	}{
		{
			name:     "url-shortener preset",
			template: "url-shortener",
			preset: func(tmpl *templates.TemplateSpec) *templates.TemplatePreset {
				return tmpl.Preset
			},
			want: []wantBinding{
				{job: "main_db", nodeID: "db-1", engine: "postgresql"},
				{job: "hot_cache", nodeID: "cache-1", engine: "redis"},
			},
		},
		{
			name:     "ecommerce preset",
			template: "ecommerce",
			preset: func(tmpl *templates.TemplateSpec) *templates.TemplatePreset {
				return tmpl.Preset
			},
			want: []wantBinding{
				{job: "main_db", nodeID: "db-1", engine: "postgresql"},
				{job: "hot_cache", nodeID: "cache-1", engine: "redis"},
				{job: "work_queue", nodeID: "mq-1", engine: "rabbitmq"},
			},
		},
		{
			name:     "social-feed preset",
			template: "social-feed",
			preset: func(tmpl *templates.TemplateSpec) *templates.TemplatePreset {
				return tmpl.Preset
			},
			want: []wantBinding{
				{job: "main_db", nodeID: "db-1", engine: "postgresql"},
				{job: "timeline", nodeID: "cache-1", engine: "redis"},
				{job: "work_queue", nodeID: "mq-1", engine: "rabbitmq"},
			},
		},
		{
			name:     "social-feed presetLite omits optional work_queue",
			template: "social-feed",
			preset: func(tmpl *templates.TemplateSpec) *templates.TemplatePreset {
				return tmpl.PresetLite
			},
			want: []wantBinding{
				{job: "main_db", nodeID: "db-1", engine: "postgresql"},
				{job: "timeline", nodeID: "cache-1", engine: "redis"},
			},
		},
		{
			name:     "social-feed-pull preset omits optional hot_cache",
			template: "social-feed-pull",
			preset: func(tmpl *templates.TemplateSpec) *templates.TemplatePreset {
				return tmpl.Preset
			},
			want: []wantBinding{
				{job: "main_db", nodeID: "db-1", engine: "postgresql"},
			},
		},
		{
			name:     "explicit override",
			template: "url-shortener",
			preset: func(tmpl *templates.TemplateSpec) *templates.TemplatePreset {
				return tmpl.Preset
			},
			mutate: func(d *DeploymentDesign) {
				d.JobBindings = map[string]string{"hot_cache": "db-1"}
			},
			want: []wantBinding{
				{job: "main_db", nodeID: "db-1", engine: "postgresql"},
				{job: "hot_cache", nodeID: "db-1", engine: "postgresql", explicit: true},
			},
		},
		{
			name:     "explicit missing node",
			template: "url-shortener",
			preset: func(tmpl *templates.TemplateSpec) *templates.TemplatePreset {
				return tmpl.Preset
			},
			mutate: func(d *DeploymentDesign) {
				d.JobBindings = map[string]string{"hot_cache": "missing-cache"}
			},
			want: []wantBinding{
				{job: "main_db", nodeID: "db-1", engine: "postgresql"},
				{job: "hot_cache", nodeID: "missing-cache", explicit: true},
			},
		},
		{
			name:     "no cache omits timeline",
			template: "social-feed",
			preset: func(tmpl *templates.TemplateSpec) *templates.TemplatePreset {
				return tmpl.Preset
			},
			mutate: func(d *DeploymentDesign) {
				filtered := d.Nodes[:0]
				for _, n := range d.Nodes {
					if n.Type != "cache" {
						filtered = append(filtered, n)
					}
				}
				d.Nodes = filtered
			},
			want: []wantBinding{
				{job: "main_db", nodeID: "db-1", engine: "postgresql"},
				{job: "work_queue", nodeID: "mq-1", engine: "rabbitmq"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpl := templates.GetTemplate(tt.template)
			if tmpl == nil {
				t.Fatalf("template %q not found", tt.template)
			}
			preset := tt.preset(tmpl)
			if preset == nil {
				t.Fatal("preset missing")
			}
			design := deploymentDesignFromTemplatePreset(tt.template, preset)
			if tt.mutate != nil {
				tt.mutate(&design)
			}
			got := ResolveJobBindings(design, tmpl)
			if len(got) != len(tt.want) {
				t.Fatalf("bindings len: want %d, got %d (%+v)", len(tt.want), len(got), got)
			}
			for i, w := range tt.want {
				b := got[i]
				if b.Job != w.job || b.NodeID != w.nodeID || b.Engine != w.engine || b.Explicit != w.explicit {
					t.Fatalf("binding[%d]: want %+v, got %+v", i, w, b)
				}
			}
		})
	}
}

func TestNodeEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		node DeploymentNode
		want string
	}{
		{
			name: "db default postgresql",
			node: DeploymentNode{Type: "db", Config: map[string]interface{}{}},
			want: "postgresql",
		},
		{
			name: "cache default redis",
			node: DeploymentNode{Type: "cache", Config: map[string]interface{}{"strategy": "cache-aside"}},
			want: "redis",
		},
		{
			name: "mq explicit rabbitmq",
			node: DeploymentNode{Type: "mq", Config: map[string]interface{}{"engine": "rabbitmq"}},
			want: "rabbitmq",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := NodeEngine(tt.node); got != tt.want {
				t.Fatalf("NodeEngine: want %q, got %q", tt.want, got)
			}
		})
	}
}

func deploymentDesignFromTemplatePreset(templateID string, preset *templates.TemplatePreset) DeploymentDesign {
	d := DeploymentDesign{
		Version:   "1.0",
		Template:  templateID,
		Archetype: "standard",
		Nodes:     make([]DeploymentNode, 0, len(preset.Nodes)),
	}
	for _, n := range preset.Nodes {
		cfg := map[string]interface{}{}
		for k, v := range n.Config {
			cfg[k] = v
		}
		pos := make([]float64, len(n.Position))
		for i, v := range n.Position {
			pos[i] = v
		}
		d.Nodes = append(d.Nodes, DeploymentNode{
			ID:       n.ID,
			Type:     canvasPresetTypeToBackend(n.Type),
			Name:     n.Label,
			Config:   cfg,
			Position: pos,
		})
	}
	for _, e := range preset.Edges {
		d.Connections = append(d.Connections, DeploymentConn{
			SourceID: e.SourceID,
			TargetID: e.TargetID,
			Protocol: e.Protocol,
		})
	}
	NormalizeDesign(&d)
	return d
}

func canvasPresetTypeToBackend(canvasType string) string {
	switch canvasType {
	case "compute":
		return "app-server"
	case "database":
		return "db"
	case "message_queue":
		return "mq"
	case "load_balancer":
		return "lb"
	default:
		return canvasType
	}
}
