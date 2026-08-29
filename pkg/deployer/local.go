package deployer

import (
	"context"
	"fmt"
	"log"
)

type LocalDeployer struct {
	// NodeType identifies what kind of component this deployer handles.
	// In production, we'd have a PostgresDeployer, RedisDeployer, etc.
	// For the mock, one struct handles all types.
	NodeType string
}

func (d *LocalDeployer) Deploy(ctx context.Context, cfg DeployConfig) error {
	// ctx (context.Context) carries a deadline and cancellation
	// signal. Always check ctx.Err() before long operations, if the context
	// is cancelled (e.g. request timed out), you should stop and return the
	// error. We arent doing long operations here, but the pattern matters.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("deploy %s: context cancelled: %w", cfg.Node.Name, err)
	}

	log.Printf("[LocalDeployer] Deploy: node=%s type=%s namespace=%s",
		cfg.Node.Name, cfg.Node.Type, cfg.Namespace)
	if cfg.Node.Type == "db" {
		invokeAfterPrimaryReady(ctx, cfg)
	}
	return nil
}

func (d *LocalDeployer) Teardown(ctx context.Context, namespace string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("teardown namespace=%s: context cancelled: %w", namespace, err)
	}

	log.Printf("[LocalDeployer] Teardown: namespace=%s", namespace)
	return nil
}

// Compile-time check: this line will fail to compile if LocalDeployer does
// not satisfy ComponentDeployer. Zero runtime cost , it's just an assignment
// to the blank identifier.
var _ ComponentDeployer = (*LocalDeployer)(nil)
