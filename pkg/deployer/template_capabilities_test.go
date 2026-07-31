package deployer

import (
	"testing"
)

func TestTemplateCapabilityWarnings(t *testing.T) {
	t.Parallel()

	mqWorkerDesign := func(template string) DeploymentDesign {
		return DeploymentDesign{
			Template: template,
			Nodes: []DeploymentNode{
				{ID: "app", Type: "app-server", Name: "app"},
				{ID: "db", Type: "db", Name: "db"},
				{ID: "mq", Type: "mq", Name: "mq"},
				{ID: "worker", Type: "worker", Name: "worker"},
			},
			Connections: []DeploymentConn{
				{SourceID: "app", TargetID: "db"},
				{SourceID: "app", TargetID: "mq"},
				{SourceID: "worker", TargetID: "mq"},
				{SourceID: "worker", TargetID: "db"},
			},
		}
	}

	tests := []struct {
		name      string
		design    DeploymentDesign
		wantWarn  bool
		wantCode  string
		wantNodes []string
	}{
		{
			name:      "url-shortener mq worker warns",
			design:    mqWorkerDesign("url-shortener"),
			wantWarn:  true,
			wantCode:  WarningCodeTemplateNoAsync,
			wantNodes: []string{"mq", "worker"},
		},
		{
			name:      "social-feed-pull mq worker warns",
			design:    mqWorkerDesign("social-feed-pull"),
			wantWarn:  true,
			wantCode:  WarningCodeTemplateNoAsync,
			wantNodes: []string{"mq", "worker"},
		},
		{
			name: "social-feed push no warn",
			design: DeploymentDesign{
				Template: "social-feed",
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "app"},
					{ID: "db", Type: "db", Name: "db"},
					{ID: "mq", Type: "mq", Name: "mq"},
					{ID: "worker", Type: "worker", Name: "worker"},
					{ID: "cache", Type: "cache", Name: "cache"},
				},
				Connections: []DeploymentConn{
					{SourceID: "app", TargetID: "db"},
					{SourceID: "app", TargetID: "mq"},
					{SourceID: "worker", TargetID: "mq"},
					{SourceID: "worker", TargetID: "cache"},
				},
			},
			wantWarn: false,
		},
		{
			name: "url-shortener without mq worker no warn",
			design: DeploymentDesign{
				Template: "url-shortener",
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "app"},
					{ID: "db", Type: "db", Name: "db"},
					{ID: "cache", Type: "cache", Name: "cache"},
				},
				Connections: []DeploymentConn{
					{SourceID: "app", TargetID: "db"},
					{SourceID: "app", TargetID: "cache"},
				},
			},
			wantWarn: false,
		},
		{
			name: "mq only still warns",
			design: DeploymentDesign{
				Template: "url-shortener",
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "app"},
					{ID: "db", Type: "db", Name: "db"},
					{ID: "mq", Type: "mq", Name: "mq"},
					{ID: "worker", Type: "worker", Name: "worker"},
				},
				Connections: []DeploymentConn{
					{SourceID: "app", TargetID: "db"},
					{SourceID: "app", TargetID: "mq"},
					{SourceID: "worker", TargetID: "mq"},
				},
			},
			wantWarn:  true,
			wantCode:  WarningCodeTemplateNoAsync,
			wantNodes: []string{"mq", "worker"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := TemplateCapabilityWarnings(tt.design)
			if !tt.wantWarn {
				if len(got) != 0 {
					t.Fatalf("warnings: want none, got %+v", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("warnings: want 1, got %d (%+v)", len(got), got)
			}
			if got[0].Code != tt.wantCode {
				t.Fatalf("code: got %q want %q", got[0].Code, tt.wantCode)
			}
			if got[0].Message == "" {
				t.Fatal("empty warning message")
			}
			if len(got[0].NodeIDs) != len(tt.wantNodes) {
				t.Fatalf("nodeIds: got %v want %v", got[0].NodeIDs, tt.wantNodes)
			}
			for i, id := range tt.wantNodes {
				if got[0].NodeIDs[i] != id {
					t.Fatalf("nodeIds[%d]: got %q want %q", i, got[0].NodeIDs[i], id)
				}
			}
		})
	}
}

func TestAssessmentMap_IncludesWarningsWhenPresent(t *testing.T) {
	t.Parallel()
	assessment := TopologyAssessment{Verdict: VerdictFull, Explanation: "ok"}
	warnings := []TopologyWarning{{Code: WarningCodeTemplateNoAsync, Message: "idle"}}
	m := AssessmentMap(assessment, warnings)
	if m["verdict"] != VerdictFull {
		t.Fatalf("verdict: got %v", m["verdict"])
	}
	w, ok := m["warnings"].([]TopologyWarning)
	if !ok || len(w) != 1 {
		t.Fatalf("warnings: got %T %v", m["warnings"], m["warnings"])
	}
}

func TestAssessmentMap_OmitsWarningsWhenEmpty(t *testing.T) {
	t.Parallel()
	m := AssessmentMap(TopologyAssessment{Verdict: VerdictFull, Explanation: "ok"}, nil)
	if _, ok := m["warnings"]; ok {
		t.Fatalf("expected no warnings key, got %v", m)
	}
}
