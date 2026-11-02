package deployer

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/client-go/kubernetes"
)

// ReplicaDelayer holds a standby's apply position behind the primary for one db engine.
type ReplicaDelayer interface {
	InitScript(hosts []string, delaysMs []int) string
	Apply(ctx context.Context, client kubernetes.Interface, namespace, host string, delayMs int) error
	Validate(config map[string]any, delaysMs []int) error
}

type postgresReplicaDelayer struct{}

func (postgresReplicaDelayer) InitScript(hosts []string, delaysMs []int) string {
	return replicaApplyDelayCaseScript(hosts, delaysMs)
}

func (postgresReplicaDelayer) Apply(ctx context.Context, client kubernetes.Interface, namespace, host string, delayMs int) error {
	admin := NewPostgresPodAdmin(client, namespace, host, dbReplicaAppLabel)
	alter := fmt.Sprintf("ALTER SYSTEM SET recovery_min_apply_delay = '%dms'", delayMs)
	if _, err := admin.ExecSQL(ctx, alter); err != nil {
		return fmt.Errorf("apply delay on %s: %w", host, err)
	}
	if _, err := admin.ExecSQL(ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("reload on %s: %w", host, err)
	}
	return nil
}

func (postgresReplicaDelayer) Validate(config map[string]any, delaysMs []int) error {
	if !HasPositiveReplicaApplyDelay(delaysMs) {
		return nil
	}
	mode, err := parseReplicationMode(config)
	if err != nil {
		return err
	}
	if !requiresSynchronousReplication(mode) {
		return nil
	}
	level, err := syncCommitLevelFromConfig(config)
	if err != nil {
		return err
	}
	if level == syncCommitRemoteApply {
		return fmt.Errorf("replicaApplyDelayMs cannot be set with syncCommitLevel remote_apply (commits would wait on the delayed standby)")
	}
	return nil
}

// LookupReplicaDelayer returns the lag-dial adapter for a normalized db engine id.
func LookupReplicaDelayer(engine string) (ReplicaDelayer, error) {
	switch normalizeReplicaDelayerEngine(engine) {
	case "postgresql":
		return postgresReplicaDelayer{}, nil
	default:
		name := strings.TrimSpace(engine)
		if name == "" {
			name = "postgresql"
		}
		return nil, fmt.Errorf("replica apply delay is not supported for db engine %q", name)
	}
}

// ApplyReplicaDelays runs Apply for each standby; stops on the first error.
func ApplyReplicaDelays(ctx context.Context, client kubernetes.Interface, namespace string, d ReplicaDelayer, hosts []string, delays []int) error {
	for i, host := range hosts {
		ms := 0
		if i < len(delays) {
			ms = delays[i]
		}
		if err := d.Apply(ctx, client, namespace, host, ms); err != nil {
			return err
		}
	}
	return nil
}

func normalizeReplicaDelayerEngine(engine string) string {
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case "", "postgres", "postgresql":
		return "postgresql"
	default:
		return strings.ToLower(strings.TrimSpace(engine))
	}
}
