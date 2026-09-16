package deployer

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	StandbyFollowStatusFollowed = "followed"
	StandbyFollowStatusRecloned = "recloned"
	StandbyFollowStatusPartial  = "partial"

	sqlSelectStreamingApplicationNames = `SELECT application_name FROM pg_stat_replication WHERE state = 'streaming'`

	defaultStandbyFollowPollDuration = 25 * time.Second
	defaultStandbyFollowPollInterval = 2 * time.Second
	defaultStandbyFollowTotalBudget  = 90 * time.Second
	defaultStandbyFollowDeleteGap    = 2 * time.Second
)

// StandbyFollowResult records how leftover standbys responded after promotion.
type StandbyFollowResult struct {
	Status             string
	ReclonedPods       []string
	PendingPods        []string
	StreamingLeftovers int
}

// StandbyFollowOpts tunes follow-or-reclone timing (zero values use production defaults).
type StandbyFollowOpts struct {
	PollDuration time.Duration
	PollInterval time.Duration
	TotalBudget  time.Duration
	DeleteGap    time.Duration
	Sleep        func(time.Duration)
	// Admin overrides the promoted-pod admin (tests only).
	Admin PostgresAdmin
}

func (o StandbyFollowOpts) pollDuration() time.Duration {
	if o.PollDuration > 0 {
		return o.PollDuration
	}
	return defaultStandbyFollowPollDuration
}

func (o StandbyFollowOpts) pollInterval() time.Duration {
	if o.PollInterval > 0 {
		return o.PollInterval
	}
	return defaultStandbyFollowPollInterval
}

func (o StandbyFollowOpts) totalBudget() time.Duration {
	if o.TotalBudget > 0 {
		return o.TotalBudget
	}
	return defaultStandbyFollowTotalBudget
}

func (o StandbyFollowOpts) deleteGap() time.Duration {
	if o.DeleteGap > 0 {
		return o.DeleteGap
	}
	return defaultStandbyFollowDeleteGap
}

func (o StandbyFollowOpts) sleep(d time.Duration) {
	if o.Sleep != nil {
		o.Sleep(d)
		return
	}
	time.Sleep(d)
}

// FollowOrRecloneStandbys polls leftover standbys for streaming replication from the
// promoted primary. Non-streaming standbys are deleted one at a time so the StatefulSet
// reclones them from Service db.
func FollowOrRecloneStandbys(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, promotedPod string,
	opts StandbyFollowOpts,
) (StandbyFollowResult, error) {
	if clientset == nil {
		return StandbyFollowResult{}, fmt.Errorf("kubernetes client is required")
	}
	if namespace == "" {
		return StandbyFollowResult{}, fmt.Errorf("namespace is required")
	}
	if promotedPod == "" {
		return StandbyFollowResult{}, fmt.Errorf("promoted pod is required")
	}

	start := time.Now()
	admin := opts.Admin
	if admin == nil {
		admin = NewPostgresPodAdmin(clientset, namespace, promotedPod, dbReplicaAppLabel)
	}

	leftovers, err := listLeftoverStandbyPods(ctx, clientset, namespace, promotedPod)
	if err != nil {
		return StandbyFollowResult{}, err
	}
	if len(leftovers) == 0 {
		return StandbyFollowResult{Status: StandbyFollowStatusFollowed}, nil
	}

	if allLeftoversStreaming(ctx, admin, leftovers, opts.pollDuration(), opts.pollInterval(), opts.sleep) {
		return StandbyFollowResult{
			Status:             StandbyFollowStatusFollowed,
			StreamingLeftovers: len(leftovers),
		}, nil
	}

	var deleted []string
	for _, name := range leftovers {
		if name == promotedPod {
			continue
		}
		if err := deleteStandbyPod(ctx, clientset, namespace, name); err != nil {
			slog.Warn("standby follow delete failed", "namespace", namespace, "pod", name, "error", err)
			continue
		}
		deleted = append(deleted, name)
		opts.sleep(opts.deleteGap())
	}

	deadline := start.Add(opts.totalBudget())
	recloned, pending := waitDeletedStandbysStreaming(ctx, admin, deleted, deadline, opts.pollInterval(), opts.sleep)

	result := StandbyFollowResult{
		ReclonedPods:       recloned,
		PendingPods:        pending,
		StreamingLeftovers: len(recloned),
	}
	switch {
	case len(pending) == 0 && len(recloned) > 0:
		result.Status = StandbyFollowStatusRecloned
	case len(pending) > 0:
		result.Status = StandbyFollowStatusPartial
	default:
		result.Status = StandbyFollowStatusPartial
		if len(deleted) > 0 && len(pending) == 0 {
			result.PendingPods = deleted
		}
	}
	return result, nil
}

func listLeftoverStandbyPods(ctx context.Context, clientset kubernetes.Interface, namespace, promotedPod string) ([]string, error) {
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s", dbReplicaAppLabel),
	})
	if err != nil {
		return nil, fmt.Errorf("list postgres replica pods: %w", err)
	}
	var names []string
	for _, pod := range pods.Items {
		if pod.Name == promotedPod {
			continue
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		names = append(names, pod.Name)
	}
	sort.Strings(names)
	return names, nil
}

func queryStreamingApplicationNames(ctx context.Context, admin PostgresAdmin) (map[string]bool, error) {
	stdout, err := admin.ExecSQL(ctx, sqlSelectStreamingApplicationNames)
	if err != nil {
		return nil, fmt.Errorf("query streaming standbys: %w", err)
	}
	out := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		out[line] = true
	}
	return out, nil
}

func allLeftoversStreaming(
	ctx context.Context,
	admin PostgresAdmin,
	leftovers []string,
	pollDuration, pollInterval time.Duration,
	sleep func(time.Duration),
) bool {
	deadline := time.Now().Add(pollDuration)
	for {
		streaming, err := queryStreamingApplicationNames(ctx, admin)
		if err == nil && everyNamePresent(leftovers, streaming) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		if !sleepWithContext(ctx, pollInterval, sleep) {
			return false
		}
	}
}

func waitDeletedStandbysStreaming(
	ctx context.Context,
	admin PostgresAdmin,
	deleted []string,
	deadline time.Time,
	pollInterval time.Duration,
	sleep func(time.Duration),
) (recloned []string, pending []string) {
	if len(deleted) == 0 {
		return nil, nil
	}
	confirmed := make(map[string]bool)
	for {
		streaming, err := queryStreamingApplicationNames(ctx, admin)
		if err == nil {
			for _, name := range deleted {
				if confirmed[name] {
					continue
				}
				if streaming[name] {
					confirmed[name] = true
				}
			}
		}
		recloned = namesFromSet(confirmed)
		pending = pendingDeleted(deleted, confirmed)
		if len(pending) == 0 || time.Now().After(deadline) {
			return recloned, pending
		}
		if !sleepWithContext(ctx, pollInterval, sleep) {
			return recloned, pendingDeleted(deleted, confirmed)
		}
	}
}

func everyNamePresent(names []string, present map[string]bool) bool {
	for _, name := range names {
		if !present[name] {
			return false
		}
	}
	return true
}

func pendingDeleted(deleted []string, confirmed map[string]bool) []string {
	var pending []string
	for _, name := range deleted {
		if !confirmed[name] {
			pending = append(pending, name)
		}
	}
	return pending
}

func namesFromSet(set map[string]bool) []string {
	var out []string
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func deleteStandbyPod(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) error {
	propagation := metav1.DeletePropagationBackground
	if err := clientset.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	}); err != nil {
		return fmt.Errorf("delete pod %s: %w", podName, err)
	}
	slog.Info("standby follow delete", "namespace", namespace, "pod", podName)
	return nil
}

func sleepWithContext(ctx context.Context, d time.Duration, sleep func(time.Duration)) bool {
	if sleep == nil {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}
	deadline := time.Now().Add(d)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		sleep(remaining)
	}
}
