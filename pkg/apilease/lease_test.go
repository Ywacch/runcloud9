package apilease

import (
	"context"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCanTake(t *testing.T) {
	const duration = 30 * time.Second
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	identity := "pod-a"
	other := "pod-b"

	fresh := now
	stale := now.Add(-duration).Add(-time.Second)

	holder := func(id string, renew time.Time) *coordinationv1.Lease {
		secs := int32(30)
		return &coordinationv1.Lease{
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &id,
				LeaseDurationSeconds: &secs,
				RenewTime:            &metav1.MicroTime{Time: renew},
			},
		}
	}

	tests := []struct {
		name  string
		lease *coordinationv1.Lease
		want  bool
	}{
		{name: "nil", lease: nil, want: true},
		{name: "this holder", lease: holder(identity, fresh), want: true},
		{name: "other fresh", lease: holder(other, fresh), want: false},
		{name: "other stale", lease: holder(other, stale), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanTake(tt.lease, identity, now, duration); got != tt.want {
				t.Fatalf("CanTake() = %v want %v", got, tt.want)
			}
		})
	}
}

func TestHold_ReleasesOnCancel(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ticker := now
	nowFn := func() time.Time { return ticker }

	client := fake.NewClientset()
	cfg := Config{
		Namespace:  "cloud9-system",
		Name:       "cloud9-api",
		Identity:   "pod-a",
		Duration:   30 * time.Second,
		RenewEvery: time.Millisecond,
		Now:        nowFn,
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := Hold(ctx, client, cfg); err != nil {
		t.Fatalf("Hold: %v", err)
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for {
		lease, err := client.CoordinationV1().Leases(cfg.Namespace).Get(context.Background(), cfg.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get lease: %v", err)
		}
		if CanTake(lease, "pod-b", ticker, cfg.Duration) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease still held by %v after cancel", lease.Spec.HolderIdentity)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHold_DoesNotSteal(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	other := "pod-other"
	secs := int32(30)
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud9-api",
			Namespace: "cloud9-system",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &other,
			LeaseDurationSeconds: &secs,
			RenewTime:            &metav1.MicroTime{Time: now},
		},
	}
	client := fake.NewClientset(existing)
	cfg := Config{
		Namespace:  "cloud9-system",
		Name:       "cloud9-api",
		Identity:   "pod-self",
		Duration:   30 * time.Second,
		RenewEvery: time.Millisecond,
		Now:        func() time.Time { return now },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := Hold(ctx, client, cfg)
	if err == nil {
		t.Fatal("expected error when lease is held")
	}
	lease, getErr := client.CoordinationV1().Leases(cfg.Namespace).Get(context.Background(), cfg.Name, metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("get lease: %v", getErr)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != other {
		t.Fatalf("holder = %v want %q", lease.Spec.HolderIdentity, other)
	}
}
