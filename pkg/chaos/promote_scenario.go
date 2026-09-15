package chaos

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/client-go/kubernetes"
)

const (
	scenarioPromoteReplica        = "promote_replica"
	scenarioPromoteLaggingReplica = "promote_lagging_replica"
)

// PromoteScenario promotes a postgres standby to primary. Terminal: Revert is a no-op.
type PromoteScenario struct {
	Client kubernetes.Interface
	Prefer deployer.CandidatePreference

	// Result is populated by Apply. Read only after Apply returns nil.
	Result deployer.PromotionResult
}

// Apply picks a standby, fences the old primary, promotes the standby, and repoints Service db.
func (s *PromoteScenario) Apply(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error {
	if s == nil || s.Client == nil {
		return fmt.Errorf("chaos: kubernetes client is required")
	}
	if targetComponent != "db" {
		return fmt.Errorf("chaos: promotion only supports targetComponent \"db\"")
	}
	if err := requireDBStreamingReplicas(design); err != nil {
		return err
	}
	active, err := ActivePartitionScenarios(ctx, s.Client, namespace)
	if err != nil {
		return err
	}
	if len(active) > 0 {
		return ErrPartitionAlreadyActive
	}

	primaryAdmin := deployer.NewPostgresPrimaryAdmin(s.Client, namespace)
	candidate, err := deployer.SelectPromotionCandidate(ctx, primaryAdmin, s.Prefer)
	if err != nil {
		return fmt.Errorf("chaos: select promotion candidate: %w", err)
	}

	preFenceLSN, err := deployer.CurrentWalLSN(ctx, primaryAdmin)
	if err != nil {
		return fmt.Errorf("chaos: read pre-fence wal lsn: %w", err)
	}

	if err := deployer.FencePrimary(ctx, s.Client, namespace); err != nil {
		return fmt.Errorf("chaos: fence primary: %w", err)
	}

	standbyAdmin := deployer.NewPostgresPodAdmin(s.Client, namespace, candidate.PodName, postgresReplicaAppLabel)
	if err := deployer.PromoteStandby(ctx, standbyAdmin); err != nil {
		return fmt.Errorf("chaos: promote standby %s: %w", candidate.PodName, err)
	}

	promotedLSN, err := deployer.CurrentWalLSN(ctx, standbyAdmin)
	if err != nil {
		return fmt.Errorf("chaos: read promoted wal lsn: %w", err)
	}

	if err := deployer.RetargetWriteService(ctx, s.Client, namespace, candidate.PodName); err != nil {
		return fmt.Errorf("chaos: retarget write service: %w", err)
	}

	walLost, err := deployer.WalBytesLost(preFenceLSN, promotedLSN)
	if err != nil {
		return fmt.Errorf("chaos: compute wal bytes lost: %w", err)
	}

	s.Result = deployer.PromotionResult{
		PromotedPod:  candidate.PodName,
		SyncState:    candidate.SyncState,
		LagBytes:     candidate.LagBytes,
		PreFenceLSN:  preFenceLSN,
		PromotedLSN:  promotedLSN,
		WalBytesLost: walLost,
	}
	return nil
}

// Revert is a no-op; promotion is terminal for the session.
func (s *PromoteScenario) Revert(context.Context, string, deployer.DeploymentDesign, string) error {
	return nil
}

// Stateful reports that promotion does not leave reversible cluster state.
func (s *PromoteScenario) Stateful() bool { return false }

// Description returns UI copy for the scenario.
func (s *PromoteScenario) Description(targetComponent string) string {
	switch s.Prefer {
	case deployer.PreferMostLagging:
		return fmt.Sprintf("Promote lagging %s replica — may destroy acked writes", targetComponent)
	default:
		return fmt.Sprintf("Promote most up-to-date %s replica to primary", targetComponent)
	}
}
