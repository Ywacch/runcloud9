package deployer

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	postgresWriteServiceName      = "db"
	postgresPrimaryDeploymentName = "postgres"
	statefulSetPodNameLabel       = "statefulset.kubernetes.io/pod-name"

	sqlSelectStreamingStandbys = `SELECT application_name, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::text
FROM pg_stat_replication
WHERE state = 'streaming'`

	sqlPromoteStandby     = "SELECT pg_promote(wait => true, wait_seconds => 30)"
	sqlCheckNotInRecovery = "SELECT pg_is_in_recovery()"
	postgresRecoveryFalse = "f"
)

// CandidatePreference selects which streaming standby to promote.
type CandidatePreference string

const (
	PreferMostCurrent CandidatePreference = "mostCurrent"
	PreferMostLagging CandidatePreference = "mostLagging"
)

// PromotionCandidate is a streaming standby eligible for promotion.
type PromotionCandidate struct {
	PodName   string
	SyncState string
	LagBytes  int64
}

// PickCandidate applies preference ordering to already-collected candidates.
func PickCandidate(candidates []PromotionCandidate, prefer CandidatePreference) (PromotionCandidate, error) {
	if len(candidates) == 0 {
		return PromotionCandidate{}, fmt.Errorf("no streaming standbys")
	}
	switch prefer {
	case PreferMostCurrent:
		return pickMostCurrent(candidates)
	case PreferMostLagging:
		return pickMostLagging(candidates)
	default:
		return PromotionCandidate{}, fmt.Errorf("unknown candidate preference %q", prefer)
	}
}

// SelectPromotionCandidate queries the live primary and picks a standby.
func SelectPromotionCandidate(ctx context.Context, admin postgresAdmin, prefer CandidatePreference) (PromotionCandidate, error) {
	stdout, err := admin.execSQL(ctx, sqlSelectStreamingStandbys)
	if err != nil {
		return PromotionCandidate{}, fmt.Errorf("list streaming standbys: %w", err)
	}
	candidates, err := parsePromotionCandidates(stdout)
	if err != nil {
		return PromotionCandidate{}, err
	}
	return PickCandidate(candidates, prefer)
}

// FencePrimary scales the postgres Deployment to zero replicas so the old primary cannot return.
func FencePrimary(ctx context.Context, clientset kubernetes.Interface, namespace string) error {
	if clientset == nil {
		return fmt.Errorf("kubernetes client is required")
	}
	dep, err := clientset.AppsV1().Deployments(namespace).Get(ctx, postgresPrimaryDeploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get postgres deployment: %w", err)
	}
	replicas := int32(0)
	dep.Spec.Replicas = &replicas
	if _, err := clientset.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("scale postgres deployment to 0: %w", err)
	}
	return nil
}

// PromoteStandby runs pg_promote on the target pod and confirms it is no longer in recovery.
func PromoteStandby(ctx context.Context, admin postgresAdmin) error {
	if _, err := admin.execSQL(ctx, sqlPromoteStandby); err != nil {
		return fmt.Errorf("pg_promote: %w", err)
	}
	stdout, err := admin.execSQL(ctx, sqlCheckNotInRecovery)
	if err != nil {
		return fmt.Errorf("check recovery state: %w", err)
	}
	if strings.TrimSpace(stdout) != postgresRecoveryFalse {
		return fmt.Errorf("standby still in recovery after pg_promote: got %q", strings.TrimSpace(stdout))
	}
	return nil
}

// RetargetWriteService patches Service db to route writes to the promoted pod.
func RetargetWriteService(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) error {
	if clientset == nil {
		return fmt.Errorf("kubernetes client is required")
	}
	if podName == "" {
		return fmt.Errorf("pod name is required")
	}
	// Strategic merge on maps keeps existing keys; set app to null so the old
	// app=postgres selector is removed and endpoints stop including the fenced primary.
	patchBody, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"selector": map[string]interface{}{
				statefulSetPodNameLabel: podName,
				"deployment":            namespace,
				"app":                   nil,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal service patch: %w", err)
	}
	_, err = clientset.CoreV1().Services(namespace).Patch(
		ctx,
		postgresWriteServiceName,
		types.StrategicMergePatchType,
		patchBody,
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("patch db service selector: %w", err)
	}
	return nil
}

func pickMostCurrent(candidates []PromotionCandidate) (PromotionCandidate, error) {
	pool := candidates
	var synced []PromotionCandidate
	for _, c := range candidates {
		if isSyncedReplicationState(c.SyncState) {
			synced = append(synced, c)
		}
	}
	if len(synced) > 0 {
		pool = synced
	}
	sort.Slice(pool, func(i, j int) bool {
		if pool[i].LagBytes != pool[j].LagBytes {
			return pool[i].LagBytes < pool[j].LagBytes
		}
		return pool[i].PodName < pool[j].PodName
	})
	return pool[0], nil
}

func pickMostLagging(candidates []PromotionCandidate) (PromotionCandidate, error) {
	pool := append([]PromotionCandidate(nil), candidates...)
	sort.Slice(pool, func(i, j int) bool {
		if pool[i].LagBytes != pool[j].LagBytes {
			return pool[i].LagBytes > pool[j].LagBytes
		}
		return pool[i].PodName < pool[j].PodName
	})
	return pool[0], nil
}

func isSyncedReplicationState(syncState string) bool {
	switch strings.ToLower(strings.TrimSpace(syncState)) {
	case "sync", "quorum":
		return true
	default:
		return false
	}
}

func parsePromotionCandidates(stdout string) ([]PromotionCandidate, error) {
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	var out []PromotionCandidate
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "|")
		if len(fields) != 3 {
			return nil, fmt.Errorf("parse standby row %q: expected 3 fields", line)
		}
		lag, err := strconv.ParseInt(strings.TrimSpace(fields[2]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse lag bytes %q: %w", fields[2], err)
		}
		out = append(out, PromotionCandidate{
			PodName:   strings.TrimSpace(fields[0]),
			SyncState: strings.TrimSpace(fields[1]),
			LagBytes:  lag,
		})
	}
	return out, nil
}
