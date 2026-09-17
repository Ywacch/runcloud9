package chaos

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/client-go/kubernetes"
)

const stallReplicaEnforceDelay = 2 * time.Second

var replicaPodNamePattern = regexp.MustCompile(`^postgres-replica-[0-9]+$`)

// DropReplication terminates the primary walsender for one replica application_name.
// Nil on PartitionScenario uses the live primary admin.
type dropReplicationFunc func(ctx context.Context, client kubernetes.Interface, namespace, applicationName string) error

func defaultDropReplication(ctx context.Context, client kubernetes.Interface, namespace, applicationName string) error {
	if err := validateReplicaPodName(applicationName); err != nil {
		return err
	}
	admin := deployer.NewPostgresPrimaryAdmin(client, namespace)
	query := fmt.Sprintf(
		"SELECT pg_terminate_backend(pid) FROM pg_stat_replication WHERE application_name = '%s'",
		applicationName,
	)
	if _, err := admin.ExecSQL(ctx, query); err != nil {
		return fmt.Errorf("chaos: terminate replication for %s: %w", applicationName, err)
	}
	return nil
}

func validateReplicaPodName(name string) error {
	if !replicaPodNamePattern.MatchString(name) {
		return fmt.Errorf("chaos: invalid replica pod name %q", name)
	}
	return nil
}

func (s *PartitionScenario) cutStalledReplicaReplication(ctx context.Context, namespace, replicaPod string) error {
	if err := validateReplicaPodName(replicaPod); err != nil {
		return err
	}
	sleep := s.Sleep
	if sleep == nil {
		sleep = time.Sleep
	}
	sleep(stallReplicaEnforceDelay)

	drop := s.DropReplication
	if drop == nil {
		drop = defaultDropReplication
	}
	if err := drop(ctx, s.Client, namespace, replicaPod); err != nil {
		return err
	}
	return nil
}
