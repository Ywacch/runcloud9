package metrics

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

func TestBuildLedger_EmitsTraceFacts(t *testing.T) {
	t.Parallel()
	l := BuildLedger(LedgerInput{
		DesignConfig: deployer.DesignConfig{Template: "url-shortener"},
		TraceSamples: []trace.JourneySample{
			{RequestID: "req-abc", Op: "/r", DurationMs: 12},
		},
	})
	if _, ok := l.Resolve(FactRef("fact:trace:req-abc")); !ok {
		t.Fatal("missing trace fact")
	}
}
