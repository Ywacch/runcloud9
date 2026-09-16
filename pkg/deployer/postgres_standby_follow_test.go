package deployer

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

type fakeStreamingAdmin struct {
	streams []string
}

func (f *fakeStreamingAdmin) ExecSQL(_ context.Context, query string) (string, error) {
	if query != sqlSelectStreamingApplicationNames {
		return "", nil
	}
	return strings.Join(f.streams, "\n"), nil
}

// streamAfterDeleteAdmin reports no streaming rows while leftover still exists, then
// the leftover application_name once the pod has been deleted (reclone complete).
type streamAfterDeleteAdmin struct {
	client kubernetes.Interface
	ns     string
	pod    string
}

func (f *streamAfterDeleteAdmin) ExecSQL(ctx context.Context, query string) (string, error) {
	if query != sqlSelectStreamingApplicationNames {
		return "", nil
	}
	_, err := f.client.CoreV1().Pods(f.ns).Get(ctx, f.pod, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return f.pod, nil
	}
	return "", err
}

func replicaPod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestFollowOrRecloneStandbys_allStreamingNoDeletes(t *testing.T) {
	t.Parallel()

	ns := "ns-follow"
	promoted := "postgres-replica-0"
	leftover := "postgres-replica-1"
	client := fake.NewClientset(
		replicaPod(ns, promoted),
		replicaPod(ns, leftover),
	)

	got, err := FollowOrRecloneStandbys(context.Background(), client, ns, promoted, StandbyFollowOpts{
		PollDuration: 5 * time.Millisecond,
		PollInterval: 1 * time.Millisecond,
		Admin:        &fakeStreamingAdmin{streams: []string{leftover}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != StandbyFollowStatusFollowed {
		t.Fatalf("status: got %q want %q", got.Status, StandbyFollowStatusFollowed)
	}
	if got.StreamingLeftovers != 1 {
		t.Fatalf("streamingLeftovers: got %d want 1", got.StreamingLeftovers)
	}

	pods, err := client.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("expected no deletes, got %d pods", len(pods.Items))
	}
}

func TestFollowOrRecloneStandbys_recloneAfterDelete(t *testing.T) {
	t.Parallel()

	ns := "ns-reclone"
	promoted := "postgres-replica-0"
	leftover := "postgres-replica-1"
	client := fake.NewClientset(
		replicaPod(ns, promoted),
		replicaPod(ns, leftover),
	)

	got, err := FollowOrRecloneStandbys(context.Background(), client, ns, promoted, StandbyFollowOpts{
		PollDuration: 1 * time.Millisecond,
		PollInterval: 1 * time.Millisecond,
		TotalBudget:  20 * time.Millisecond,
		DeleteGap:    1 * time.Millisecond,
		Sleep:        func(time.Duration) {},
		Admin: &streamAfterDeleteAdmin{
			client: client,
			ns:     ns,
			pod:    leftover,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != StandbyFollowStatusRecloned {
		t.Fatalf("status: got %q want %q", got.Status, StandbyFollowStatusRecloned)
	}
	if len(got.ReclonedPods) != 1 || got.ReclonedPods[0] != leftover {
		t.Fatalf("reclonedPods: %+v", got.ReclonedPods)
	}
	if _, err := client.CoreV1().Pods(ns).Get(context.Background(), leftover, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected leftover deleted, get err=%v", err)
	}
}

func TestFollowOrRecloneStandbys_partialOnCap(t *testing.T) {
	t.Parallel()

	ns := "ns-partial"
	promoted := "postgres-replica-0"
	leftover := "postgres-replica-1"
	client := fake.NewClientset(
		replicaPod(ns, promoted),
		replicaPod(ns, leftover),
	)

	got, err := FollowOrRecloneStandbys(context.Background(), client, ns, promoted, StandbyFollowOpts{
		PollDuration: 1 * time.Millisecond,
		PollInterval: 1 * time.Millisecond,
		TotalBudget:  1 * time.Millisecond,
		DeleteGap:    1 * time.Millisecond,
		Sleep:        func(time.Duration) {},
		Admin:        &fakeStreamingAdmin{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != StandbyFollowStatusPartial {
		t.Fatalf("status: got %q want %q", got.Status, StandbyFollowStatusPartial)
	}
	if len(got.PendingPods) != 1 || got.PendingPods[0] != leftover {
		t.Fatalf("pendingPods: %+v", got.PendingPods)
	}
	if got.StreamingLeftovers != 0 {
		t.Fatalf("streamingLeftovers: got %d want 0", got.StreamingLeftovers)
	}
}

func TestFollowOrRecloneStandbys_neverDeletesPromotedPod(t *testing.T) {
	t.Parallel()

	ns := "ns-safe"
	promoted := "postgres-replica-0"
	client := fake.NewClientset(replicaPod(ns, promoted))

	got, err := FollowOrRecloneStandbys(context.Background(), client, ns, promoted, StandbyFollowOpts{
		Admin: &fakeStreamingAdmin{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != StandbyFollowStatusFollowed {
		t.Fatalf("status: got %q", got.Status)
	}
	if _, err := client.CoreV1().Pods(ns).Get(context.Background(), promoted, metav1.GetOptions{}); err != nil {
		t.Fatalf("promoted pod deleted: %v", err)
	}
}

func TestListLeftoverStandbyPods_skipsPromotedAndDeleting(t *testing.T) {
	t.Parallel()

	ns := "ns-list"
	promoted := "postgres-replica-0"
	deleting := replicaPod(ns, "postgres-replica-2")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now

	client := fake.NewClientset(
		replicaPod(ns, promoted),
		replicaPod(ns, "postgres-replica-1"),
		deleting,
	)

	got, err := listLeftoverStandbyPods(context.Background(), client, ns, promoted)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != "postgres-replica-1" {
		t.Fatalf("leftovers: %+v", got)
	}
}
