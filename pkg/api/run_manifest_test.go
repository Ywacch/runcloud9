package api

import (
	"context"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/manifest"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestAdmitAndCreate_StampsManifest(t *testing.T) {
	t.Parallel()

	ns := "ns-manifest-stamp"
	s := newFakeDeploymentStore()
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	h.manifestRNG = func() int64 { return 4242 }

	design := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "standard",
		Nodes: []deployer.DeploymentNode{{
			ID: "db1", Type: "db",
			Config: map[string]interface{}{"engine": "postgresql"},
		}},
	}
	status, _, err := h.admitAndCreate(
		context.Background(),
		"user-1",
		2,
		0,
		design,
		"explorer",
		ns,
		time.Now().Add(time.Hour),
		true,
	)
	if err != nil {
		t.Fatalf("admitAndCreate: %v", err)
	}
	h.finishAdmit(context.Background(), "user-1", design, ns, status, 0)
	row, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := row.MetricsSummary["run_manifest"]
	if !ok {
		t.Fatalf("summary: %+v", row.MetricsSummary)
	}
	rm, err := manifest.DecodeSummary(raw)
	if err != nil {
		t.Fatal(err)
	}
	if rm.DesignHash == "" {
		t.Fatal("expected designHash")
	}
	if rm.RNGSeed != 4242 {
		t.Fatalf("rngSeed = %d", rm.RNGSeed)
	}
}

func TestProvisionRunningStampPreservesManifest(t *testing.T) {
	t.Parallel()

	ns := "ns-preserve-manifest"
	s := newFakeDeploymentStore()
	s.byNS[ns] = storeDeploymentRowWithSummary(ns, map[string]any{
		"queued_at": "2026-06-01T12:00:00Z",
		"run_manifest": map[string]any{
			"schemaVersion": manifest.SchemaVersion,
			"designHash":    "sha256:deadbeef",
		},
	})
	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	h.stampMetrics(context.Background(), ns, map[string]any{
		"seeded":     true,
		"running_at": "2026-06-01T12:05:00Z",
	})
	row, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if row.MetricsSummary["queued_at"] == nil {
		t.Fatal("queued_at dropped")
	}
	rm, err := manifest.DecodeSummary(row.MetricsSummary["run_manifest"])
	if err != nil {
		t.Fatal(err)
	}
	if rm.DesignHash != "sha256:deadbeef" {
		t.Fatalf("run_manifest: %+v", rm)
	}
	if row.MetricsSummary["seeded"] != true {
		t.Fatal("seeded missing")
	}
}

func storeDeploymentRowWithSummary(ns string, summary map[string]any) store.DeploymentRow {
	return store.DeploymentRow{
		Namespace:      ns,
		UserID:         "user-1",
		Template:       "url-shortener",
		Status:         "provisioning",
		MetricsSummary: summary,
	}
}
