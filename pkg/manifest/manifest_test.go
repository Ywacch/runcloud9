package manifest

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestDesignHash_IgnoresPositionAndLabels(t *testing.T) {
	t.Parallel()
	base := deployer.DeploymentDesign{
		Template: "social-feed",
		Nodes: []deployer.DeploymentNode{{
			ID:       "db1",
			Type:     "db",
			Name:     "Primary DB",
			Position: []float64{10, 20},
			Config:   map[string]interface{}{"readReplicas": float64(1)},
		}},
	}
	moved := base
	moved.Nodes[0].Position = []float64{99, 99}
	moved.Nodes[0].Name = "Renamed"

	h1, err := DesignHash(base)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := DesignHash(moved)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Fatalf("hash changed: %q vs %q", h1, h2)
	}
}

func TestDesignHash_OrderIndependent(t *testing.T) {
	t.Parallel()
	a := deployer.DeploymentDesign{
		Template: "social-feed",
		Nodes: []deployer.DeploymentNode{
			{ID: "app1", Type: "app-server", Config: map[string]interface{}{}},
			{ID: "db1", Type: "db", Config: map[string]interface{}{"readReplicas": float64(1)}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app1", TargetID: "db1", Protocol: "tcp"},
		},
	}
	b := deployer.DeploymentDesign{
		Template: "social-feed",
		Nodes: []deployer.DeploymentNode{
			{ID: "db1", Type: "db", Config: map[string]interface{}{"readReplicas": float64(1)}},
			{ID: "app1", Type: "app-server", Config: map[string]interface{}{}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app1", TargetID: "db1", Protocol: "tcp"},
		},
	}
	h1, err := DesignHash(a)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := DesignHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Fatalf("order changed hash: %q vs %q", h1, h2)
	}
}

func TestDesignHash_ConfigChangeChangesHash(t *testing.T) {
	t.Parallel()
	low := deployer.DeploymentDesign{
		Template: "social-feed",
		Nodes: []deployer.DeploymentNode{{
			ID: "db1", Type: "db",
			Config: map[string]interface{}{"readReplicas": float64(1)},
		}},
	}
	high := deployer.DeploymentDesign{
		Template: "social-feed",
		Nodes: []deployer.DeploymentNode{{
			ID: "db1", Type: "db",
			Config: map[string]interface{}{"readReplicas": float64(2)},
		}},
	}

	h1, err := DesignHash(low)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := DesignHash(high)
	if err != nil {
		t.Fatal(err)
	}
	if h1 == h2 {
		t.Fatal("expected different hash for readReplicas change")
	}
}

func TestDesignHash_AliasNormalized(t *testing.T) {
	t.Parallel()
	canon := deployer.DeploymentDesign{
		Template: "social-feed",
		Nodes: []deployer.DeploymentNode{{
			ID: "lb1", Type: "lb",
			Config: map[string]interface{}{"algorithm": "round_robin"},
		}},
	}
	alias := deployer.DeploymentDesign{
		Template: "social-feed",
		Nodes: []deployer.DeploymentNode{{
			ID: "lb1", Type: "load_balancer",
			Config: map[string]interface{}{"algorithm": "round_robin"},
		}},
	}
	h1, err := DesignHash(canon)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := DesignHash(alias)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Fatalf("alias hash mismatch: %q vs %q", h1, h2)
	}
}

func TestBuild_Fields(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		in    Input
		check func(t *testing.T, m RunManifest)
	}{
		{
			name: "defaults",
			in: Input{
				TemplateID:      "url-shortener",
				TemplateVersion: "sha256:abc",
				ImageVersions:   map[string]string{"appServer": "img:1", "loadgen": "lg:1"},
				SeedRef:         "gs://bucket/dump.sql",
				Design: deployer.DeploymentDesign{
					Template: "url-shortener",
					Nodes:    []deployer.DeploymentNode{{ID: "db1", Type: "db", Config: map[string]interface{}{}}},
				},
				RNGSeed: 42,
				Now:     now,
			},
			check: func(t *testing.T, m RunManifest) {
				if m.SchemaVersion != SchemaVersion {
					t.Fatalf("schemaVersion: %d", m.SchemaVersion)
				}
				if m.TemplateID != "url-shortener" || m.TemplateVersion != "sha256:abc" {
					t.Fatalf("template: %+v", m)
				}
				if m.RNGSeed != 42 || m.SeedRef != "gs://bucket/dump.sql" {
					t.Fatalf("seed/rng: %+v", m)
				}
				if m.DesignHash == "" || m.CreatedAt != now {
					t.Fatalf("hash/created: %+v", m)
				}
				if m.ImageVersions["appServer"] != "img:1" {
					t.Fatalf("images: %+v", m.ImageVersions)
				}
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := Build(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, m)
		})
	}
}
