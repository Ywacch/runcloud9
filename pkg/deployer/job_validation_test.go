package deployer

import (
	"errors"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/templates"
)

func TestValidateJobBindings(t *testing.T) {
	t.Parallel()

	cat := engines.Default()
	sf := templates.GetTemplate("social-feed")
	if sf == nil {
		t.Fatal("social-feed template missing")
	}

	tests := []struct {
		name    string
		design  DeploymentDesign
		tmpl    *templates.TemplateSpec
		catalog *engines.Catalog
		wantErr error
		wantMsg string
	}{
		{
			name: "timeline unbound",
			design: DeploymentDesign{
				Template: "social-feed",
				Nodes: []DeploymentNode{
					{ID: "w1", Type: "worker", Name: "Worker"},
					{ID: "db1", Type: "db", Name: "DB"},
					{ID: "mq1", Type: "mq", Name: "MQ"},
				},
				Connections: []DeploymentConn{
					{SourceID: "w1", TargetID: "db1"},
					{SourceID: "w1", TargetID: "mq1"},
				},
			},
			tmpl:    sf,
			wantErr: ErrJobUnbound,
			wantMsg: "timeline job is not bound",
		},
		{
			name: "timeline bound with cache passes",
			design: DeploymentDesign{
				Template: "social-feed",
				Nodes: []DeploymentNode{
					{ID: "w1", Type: "worker", Name: "Worker"},
					{ID: "db1", Type: "db", Name: "DB"},
					{ID: "mq1", Type: "mq", Name: "MQ"},
					{ID: "c1", Type: "cache", Name: "Cache", Config: map[string]interface{}{
						"strategy": "timeline-store", "cacheStrategy": "timeline-store",
					}},
				},
				Connections: []DeploymentConn{
					{SourceID: "w1", TargetID: "db1"},
					{SourceID: "w1", TargetID: "mq1"},
					{SourceID: "w1", TargetID: "c1"},
				},
			},
			tmpl: sf,
		},
		{
			name: "engine not allowed",
			design: func() DeploymentDesign {
				d := deploymentDesignFromTemplatePreset("url-shortener", templates.GetTemplate("url-shortener").Preset)
				d.JobBindings = map[string]string{"hot_cache": "db-1"}
				return d
			}(),
			tmpl:    templates.GetTemplate("url-shortener"),
			wantErr: ErrEngineNotAllowed,
			wantMsg: "hot_cache cannot use postgresql",
		},
		{
			name: "missing bound node",
			design: func() DeploymentDesign {
				d := deploymentDesignFromTemplatePreset("url-shortener", templates.GetTemplate("url-shortener").Preset)
				d.JobBindings = map[string]string{"hot_cache": "missing-node"}
				return d
			}(),
			tmpl:    templates.GetTemplate("url-shortener"),
			wantErr: ErrBindingNoNode,
			wantMsg: "bound to missing node",
		},
		{
			name: "impossible pair uses catalog reason",
			design: func() DeploymentDesign {
				us := templates.GetTemplate("url-shortener")
				d := deploymentDesignFromTemplatePreset("url-shortener", us.Preset)
				for i := range d.Nodes {
					if d.Nodes[i].ID == "cache-1" {
						d.Nodes[i].Config["engine"] = "memcached"
					}
				}
				d.JobBindings = map[string]string{"hot_cache": "cache-1"}
				return d
			}(),
			tmpl: func() *templates.TemplateSpec {
				tmpl := *templates.GetTemplate("url-shortener")
				jobs := append([]templates.JobRequirement(nil), tmpl.Jobs...)
				for i := range jobs {
					if jobs[i].ID == "hot_cache" {
						jobs[i].AllowedEngines = []string{"memcached"}
					}
				}
				tmpl.Jobs = jobs
				return &tmpl
			}(),
			catalog: func() *engines.Catalog {
				c := *engines.Default()
				pairs := make([]engines.Pair, 0, len(c.Pairs))
				for _, p := range c.Pairs {
					if p.Job == "hot_cache" && p.Engine == "memcached" {
						continue
					}
					pairs = append(pairs, p)
				}
				pairs = append(pairs, engines.Pair{
					Job: "hot_cache", Engine: "memcached", Fit: engines.FitImpossible,
					Build: engines.BuildAvailable, Reason: "no keyed point read", Fidelity: engines.FidelityReal,
				})
				c.Pairs = pairs
				return &c
			}(),
			wantErr: ErrImpossiblePair,
			wantMsg: "no keyed point read",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			catalog := cat
			if tt.catalog != nil {
				catalog = tt.catalog
			}
			err := ValidateJobBindings(tt.design, tt.tmpl, catalog)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want error %v", tt.wantErr)
			}
			var jb *JobBindingError
			if !errors.As(err, &jb) {
				t.Fatalf("want *JobBindingError, got %T: %v", err, err)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("want errors.Is %v, got %v (%v)", tt.wantErr, jb.Err, err)
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("message %q not in %q", tt.wantMsg, err.Error())
			}
		})
	}
}

func TestValidateJobBindings_EmbeddedPresetsPass(t *testing.T) {
	t.Parallel()

	for _, tmpl := range templates.Registry() {
		if !tmpl.Deployable {
			continue
		}
		t.Run(tmpl.ID+"/preset", func(t *testing.T) {
			t.Parallel()
			if tmpl.Preset == nil {
				t.Skip("no preset")
			}
			design := deploymentDesignFromTemplatePreset(tmpl.ID, tmpl.Preset)
			spec := templates.GetTemplate(tmpl.ID)
			if spec == nil {
				t.Fatal("template missing from registry")
			}
			if err := ValidateJobBindings(design, spec, engines.Default()); err != nil {
				t.Fatalf("preset: %v", err)
			}
			if err := ValidateRuntimeContract(design, spec); err != nil {
				t.Fatalf("runtime contract: %v", err)
			}
		})
	}
}
