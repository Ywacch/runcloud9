package metrics

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestBuildLedger_EmitsWiringAndLoadgenMixFacts(t *testing.T) {
	t.Parallel()
	design := &deployer.DeploymentDesign{
		Template: "url-shortener",
		Nodes: []deployer.DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "a", TargetID: "d", Protocol: "TCP"},
		},
	}
	l := BuildLedger(LedgerInput{
		DesignConfig: deployer.DesignConfig{Template: "url-shortener"},
		Design:       design,
	})
	if _, ok := l.Resolve(FactRef("fact:wiring:a->d.DB_HOST")); !ok {
		t.Fatal("missing wiring DB_HOST fact")
	}
	if _, ok := l.Resolve(FactRef("fact:config:loadgen.mix.r.code")); !ok {
		t.Fatal("missing loadgen mix fact for GET /r/{code}")
	}
}
