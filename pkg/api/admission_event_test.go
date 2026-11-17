package api

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestDurationMillis(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		d    time.Duration
		want int
	}{
		{name: "zero", d: 0, want: 0},
		{name: "negative", d: -time.Second, want: 0},
		{name: "one and a half seconds", d: 1500 * time.Millisecond, want: 1500},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := durationMillis(tt.d); got != tt.want {
				t.Fatalf("durationMillis(%v) = %d want %d", tt.d, got, tt.want)
			}
		})
	}
}

func copyAdmissionEvents(s *fakeDeploymentStore) []store.AdmissionEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]store.AdmissionEvent, len(s.events))
	copy(out, s.events)
	return out
}

func admissionKinds(events []store.AdmissionEvent) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Kind)
	}
	return out
}

func TestRecordAdmissionEvent_IgnoresInsertError(t *testing.T) {
	t.Parallel()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	h.recordAdmissionEvent(context.Background(), store.AdmissionEvent{
		Kind: store.AdmissionKindQueueFull,
	})
}

func TestRunBackgroundProvision_RecordsDoorWaitWhenBlocked(t *testing.T) {
	s := newFakeDeploymentStore()
	ns := "ns-door-wait"
	s.users = map[string]store.UserRecord{"u1": {IsInternal: true}}
	_, err := s.CreateDeployment(context.Background(), store.CreateDeploymentParams{
		Namespace: ns,
		Template:  "url-shortener",
		UserID:    "u1",
		Design:    map[string]any{"template": "url-shortener"},
		ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("CreateDeployment: %v", err)
	}

	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	h.door = newProvisionDoor(1, 240)
	hold, err := h.door.Acquire(context.Background(), "holder", true, nil)
	if err != nil {
		t.Fatalf("hold door: %v", err)
	}

	design := deployer.DeploymentDesign{Template: "url-shortener"}
	done := make(chan struct{})
	go func() {
		h.runBackgroundProvision(design, ns, "u1", "explorer", time.Now().Add(9*time.Minute), 9)
		close(done)
	}()

	deadline := time.Now().Add(3 * time.Second)
	var sawWait bool
	for time.Now().Before(deadline) {
		if slices.Contains(admissionKinds(copyAdmissionEvents(s)), store.AdmissionKindDoorWait) {
			sawWait = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sawWait {
		hold()
		t.Fatalf("kinds = %v, want door_wait", admissionKinds(copyAdmissionEvents(s)))
	}

	row, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		hold()
		t.Fatal(err)
	}
	if row.MetricsSummary["door_wait_started_at"] == nil {
		hold()
		t.Fatalf("metrics_summary missing door_wait_started_at: %+v", row.MetricsSummary)
	}

	hold()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runBackgroundProvision did not finish")
	}

	kinds := admissionKinds(copyAdmissionEvents(s))
	if !slices.Contains(kinds, store.AdmissionKindDoorAcquired) {
		t.Fatalf("kinds = %v, want door_acquired", kinds)
	}
	row, err = s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if row.MetricsSummary["door_acquired_at"] == nil {
		t.Fatalf("metrics_summary missing door_acquired_at: %+v", row.MetricsSummary)
	}
}
