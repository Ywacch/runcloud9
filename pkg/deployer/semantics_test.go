package deployer

import (
	"strings"
	"testing"
)

func TestConnectionRules_CompleteGrid(t *testing.T) {
	types := []string{"lb", "app-server", "db", "cache", "mq", "worker"}
	for _, a := range types {
		for _, b := range types {
			key := [2]string{a, b}
			if _, ok := connectionRules[key]; !ok {
				t.Fatalf("missing rule for %s → %s", a, b)
			}
		}
	}
	if len(connectionRules) != 36 {
		t.Fatalf("expected 36 rules, got %d", len(connectionRules))
	}
}

func TestAssessTopology_URLShortenerPreset(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "lb", Type: "lb", Name: "lb"},
			{ID: "app", Type: "app-server", Name: "app"},
			{ID: "db", Type: "db", Name: "db"},
			{ID: "cache", Type: "cache", Name: "cache"},
		},
		Connections: []DeploymentConn{
			{SourceID: "lb", TargetID: "app"},
			{SourceID: "app", TargetID: "db"},
			{SourceID: "app", TargetID: "cache"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictFull {
		t.Fatalf("verdict: got %q want %q (%s)", a.Verdict, VerdictFull, a.Explanation)
	}
	if len(a.Deployable) != 4 {
		t.Fatalf("deployable: got %d nodes", len(a.Deployable))
	}
}

func TestAssessTopology_SocialFeedPreset(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "lb", Type: "lb", Name: "lb"},
			{ID: "app", Type: "app-server", Name: "app"},
			{ID: "mq", Type: "mq", Name: "mq"},
			{ID: "worker", Type: "worker", Name: "worker"},
			{ID: "db", Type: "db", Name: "db"},
			{ID: "cache", Type: "cache", Name: "cache"},
		},
		Connections: []DeploymentConn{
			{SourceID: "lb", TargetID: "app"},
			{SourceID: "app", TargetID: "mq"},
			{SourceID: "worker", TargetID: "mq"},
			{SourceID: "worker", TargetID: "db"},
			{SourceID: "app", TargetID: "cache"},
			{SourceID: "app", TargetID: "db"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictFull {
		t.Fatalf("verdict: got %q (%s)", a.Verdict, a.Explanation)
	}
}

func TestAssessTopology_InvalidLbToDb(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "lb", Type: "lb", Name: "lb"},
			{ID: "db", Type: "db", Name: "db"},
		},
		Connections: []DeploymentConn{
			{SourceID: "lb", TargetID: "db"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictInvalid {
		t.Fatalf("verdict: got %q", a.Verdict)
	}
	if !strings.Contains(a.Explanation, "Load balancers") {
		t.Fatalf("explanation: %q", a.Explanation)
	}
}

func TestAssessTopology_InvalidDbToAppServer(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "db", Type: "db", Name: "db"},
			{ID: "app", Type: "app-server", Name: "app"},
		},
		Connections: []DeploymentConn{
			{SourceID: "db", TargetID: "app"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictInvalid || !strings.Contains(a.Explanation, "passive") {
		t.Fatalf("got verdict=%q explanation=%q", a.Verdict, a.Explanation)
	}
}

func TestAssessTopology_InvalidWorkerToLb(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "w", Type: "worker", Name: "w"},
			{ID: "lb", Type: "lb", Name: "lb"},
			{ID: "mq", Type: "mq", Name: "mq"},
		},
		Connections: []DeploymentConn{
			{SourceID: "w", TargetID: "lb"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictInvalid {
		t.Fatalf("verdict: got %q (%s)", a.Verdict, a.Explanation)
	}
}

func TestAssessTopology_AmbiguityCacheWithoutDb(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app"},
			{ID: "cache", Type: "cache", Name: "cache"},
		},
		Connections: []DeploymentConn{
			{SourceID: "app", TargetID: "cache"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictInvalid || !strings.Contains(a.Explanation, "cache") {
		t.Fatalf("got verdict=%q explanation=%q", a.Verdict, a.Explanation)
	}
}

func TestAssessTopology_AmbiguityWorkerWithoutMq(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app"},
			{ID: "w", Type: "worker", Name: "w"},
			{ID: "db", Type: "db", Name: "db"},
		},
		Connections: []DeploymentConn{
			{SourceID: "app", TargetID: "db"},
			{SourceID: "w", TargetID: "db"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictInvalid || !strings.Contains(a.Explanation, "message queue") {
		t.Fatalf("got verdict=%q explanation=%q", a.Verdict, a.Explanation)
	}
}

func TestAssessTopology_AmbiguityMqWithoutWorker(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app"},
			{ID: "mq", Type: "mq", Name: "mq"},
			{ID: "db", Type: "db", Name: "db"},
		},
		Connections: []DeploymentConn{
			{SourceID: "app", TargetID: "mq"},
			{SourceID: "app", TargetID: "db"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictInvalid || !strings.Contains(a.Explanation, "consumer") {
		t.Fatalf("got verdict=%q explanation=%q", a.Verdict, a.Explanation)
	}
}

func TestAssessTopology_SkipsClientEdges(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "c", Type: "client", Name: "traffic"},
			{ID: "lb", Type: "lb", Name: "lb"},
			{ID: "app", Type: "app-server", Name: "app"},
			{ID: "db", Type: "db", Name: "db"},
		},
		Connections: []DeploymentConn{
			{SourceID: "c", TargetID: "lb"},
			{SourceID: "lb", TargetID: "app"},
			{SourceID: "app", TargetID: "db"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictFull {
		t.Fatalf("verdict: got %q (%s)", a.Verdict, a.Explanation)
	}
}

func TestAssessTopology_UnknownSourceNode(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app"},
		},
		Connections: []DeploymentConn{
			{SourceID: "missing", TargetID: "app"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictInvalid || !strings.Contains(a.Explanation, "unknown source") {
		t.Fatalf("got verdict=%q explanation=%q", a.Verdict, a.Explanation)
	}
}

func TestAssessTopology_AppServerToAppServerNotWired(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "a"},
			{ID: "b", Type: "app-server", Name: "b"},
			{ID: "db", Type: "db", Name: "db"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "b"},
			{SourceID: "a", TargetID: "db"},
			{SourceID: "b", TargetID: "db"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictPartial {
		t.Fatalf("got verdict=%q explanation=%q", a.Verdict, a.Explanation)
	}
	if !strings.Contains(a.Explanation, "not wired") && !strings.Contains(a.Explanation, "simulated") {
		t.Fatalf("explanation: %q", a.Explanation)
	}
	if len(a.Deployable) != 1 || a.Deployable[0].Type != "db" {
		t.Fatalf("deployable: %+v", a.Deployable)
	}
	if len(a.Simulatable) != 2 {
		t.Fatalf("simulatable: %+v", a.Simulatable)
	}
}

func TestAssessTopology_LbToLbSimulateOnly(t *testing.T) {
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "lb1", Type: "lb", Name: "lb1"},
			{ID: "lb2", Type: "lb", Name: "lb2"},
		},
		Connections: []DeploymentConn{
			{SourceID: "lb1", TargetID: "lb2"},
		},
	}
	a := AssessTopology(d)
	if a.Verdict != VerdictSimulate {
		t.Fatalf("got verdict=%q explanation=%q", a.Verdict, a.Explanation)
	}
	if !strings.Contains(a.Explanation, "Chained load balancers") {
		t.Fatalf("explanation: %q", a.Explanation)
	}
	if len(a.Deployable) != 0 || len(a.Simulatable) != 2 {
		t.Fatalf("deployable=%d simulatable=%d", len(a.Deployable), len(a.Simulatable))
	}
}
