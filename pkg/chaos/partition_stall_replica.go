package chaos

import (
	"context"
	"fmt"
	"regexp"
	"strings"
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

func isFakeKubernetesClient(client kubernetes.Interface) bool {
	if client == nil {
		return false
	}
	return strings.Contains(fmt.Sprintf("%T", client), "fake.Clientset")
}

func noopStallReplicaSleep(time.Duration) {}

func noopDropReplication(context.Context, kubernetes.Interface, string, string) error {
	return nil
}

func (s *PartitionScenario) cutStalledReplicaReplication(ctx context.Context, namespace, replicaPod string) error {
	if err := validateReplicaPodName(replicaPod); err != nil {
		return err
	}
	sleep := s.Sleep
	if sleep == nil {
		if isFakeKubernetesClient(s.Client) {
			sleep = noopStallReplicaSleep
		} else {
			sleep = time.Sleep
		}
	}
	sleep(stallReplicaEnforceDelay)

	drop := s.DropReplication
	if drop == nil {
		if isFakeKubernetesClient(s.Client) {
			drop = noopDropReplication
		} else {
			drop = defaultDropReplication
		}
	}
	if err := drop(ctx, s.Client, namespace, replicaPod); err != nil {
		return err
	}
	return nil
}
