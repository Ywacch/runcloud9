package api

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/store"
)

type fakeComponentDeployer struct {
	deployFn func(ctx context.Context, cfg deployer.DeployConfig) error
}

func (f *fakeComponentDeployer) Deploy(ctx context.Context, cfg deployer.DeployConfig) error {
	if f.deployFn != nil {
		return f.deployFn(ctx, cfg)
	}
	return nil
}

func (f *fakeComponentDeployer) Teardown(context.Context, string) error { return nil }

type fakeSeeder struct {
	prePath      string
	preErr       error
	seedErr      error
	seedStarted  chan struct{}
	seedStartedM sync.Once
}

func (f *fakeSeeder) SeedDatabase(context.Context, string, string) error { return errors.New("unused") }

func (f *fakeSeeder) PreDownloadDump(context.Context, string) (string, error) {
	return f.prePath, f.preErr
}

func (f *fakeSeeder) SeedWithDump(context.Context, string, string, string) error {
	f.seedStartedM.Do(func() { close(f.seedStarted) })
	return f.seedErr
}

func TestOrchestrate_SeedingStartsAfterDBBeforeLaterLayers(t *testing.T) {
	s := newFakeDeploymentStore()
	ns := "ns-seed-order"
	_, _ = s.CreateDeployment(context.Background(), store.CreateDeploymentParams{
		Namespace: ns, Template: "url-shortener", Archetype: "default", Design: map[string]any{}, ExpiresAt: time.Now().Add(time.Minute),
	})

	appBlock := make(chan struct{})
	deployers := map[string]deployer.ComponentDeployer{
		"db": &fakeComponentDeployer{},
		"app-server": &fakeComponentDeployer{
			deployFn: func(ctx context.Context, cfg deployer.DeployConfig) error {
				select {
				case <-appBlock:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
	}
	seed := &fakeSeeder{
		prePath:     "dump.tmp",
		seedStarted: make(chan struct{}),
	}

	h := NewHandler(deployers, s, nil, nil, nil, nil, nil, nil, nil)
	h.seeder = seed

	design := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "default",
		Nodes: []deployer.DeploymentNode{
			{ID: "db-1", Type: "db", Name: "DB"},
			{ID: "app-1", Type: "app-server", Name: "App"},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app-1", TargetID: "db-1"},
		},
	}

	done := make(chan error, 1)
	go func() {
		_, err := h.orchestrate(context.Background(), design, ns)
		done <- err
	}()

	select {
	case <-seed.seedStarted:
		// Expected: seeding starts while app layer is blocked.
	case <-time.After(2 * time.Second):
		t.Fatal("expected seeding to start before app layer unblocked")
	}

	close(appBlock)
	if err := <-done; err != nil {
		t.Fatalf("orchestrate returned error: %v", err)
	}
}

func TestOrchestrate_SeedFailureNonFatalWithStatus(t *testing.T) {
	s := newFakeDeploymentStore()
	ns := "ns-seed-fail"
	_, _ = s.CreateDeployment(context.Background(), store.CreateDeploymentParams{
		Namespace: ns, Template: "url-shortener", Archetype: "default", Design: map[string]any{}, ExpiresAt: time.Now().Add(time.Minute),
	})

	deployers := map[string]deployer.ComponentDeployer{
		"db":         &fakeComponentDeployer{},
		"app-server": &fakeComponentDeployer{},
	}
	seed := &fakeSeeder{
		prePath:     "dump.tmp",
		seedErr:     errors.New("restore failed"),
		seedStarted: make(chan struct{}),
	}
	h := NewHandler(deployers, s, nil, nil, nil, nil, nil, nil, nil)
	h.seeder = seed

	design := deployer.DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "default",
		Nodes: []deployer.DeploymentNode{
			{ID: "db-1", Type: "db", Name: "DB"},
			{ID: "app-1", Type: "app-server", Name: "App"},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app-1", TargetID: "db-1"},
		},
	}

	res, err := h.orchestrate(context.Background(), design, ns)
	if err != nil {
		t.Fatalf("orchestrate returned error: %v", err)
	}
	if res.Seeded {
		t.Fatalf("expected seeded=false on seed error, got %+v", res)
	}

	row, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got, _ := row.MetricsSummary["seeding_status"].(string); got != "failed" {
		t.Fatalf("seeding_status: want failed, got %q", got)
	}
	if got, _ := row.MetricsSummary["provision_phase"].(string); got != "running" {
		t.Fatalf("provision_phase: want running, got %q", got)
	}
}
