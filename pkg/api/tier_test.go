package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

type stubUserStore struct {
	user store.UserRecord
	err  error
}

func (s *stubUserStore) ListActiveDeployments(context.Context, string) ([]store.ActiveDeploymentRow, error) {
	return nil, nil
}
func (s *stubUserStore) ListUserDeploymentHistory(context.Context, string, int) ([]store.DeploymentHistoryRow, error) {
	return nil, nil
}
func (s *stubUserStore) ListQueuedDeployments(context.Context) ([]store.QueuedDeploymentRow, error) {
	return nil, nil
}
func (s *stubUserStore) CountQueuedDeployments(context.Context) (int, error) { return 0, nil }
func (s *stubUserStore) ClaimQueued(context.Context, string) (bool, error)  { return false, nil }
func (s *stubUserStore) CreateDeployment(context.Context, store.CreateDeploymentParams) (string, error) {
	return "", nil
}
func (s *stubUserStore) SumActiveAdmissionWeights(context.Context) (store.AdmissionOccupancy, error) {
	return store.AdmissionOccupancy{}, nil
}
func (s *stubUserStore) UpdateStatus(context.Context, string, string) error { return nil }
func (s *stubUserStore) UpdateExpiresAt(context.Context, string, time.Time) error {
	return nil
}
func (s *stubUserStore) UpdateMetricsSummary(context.Context, string, map[string]any) error {
	return nil
}
func (s *stubUserStore) GetDeployment(context.Context, string) (store.DeploymentRow, error) {
	return store.DeploymentRow{}, errors.New("not found")
}
func (s *stubUserStore) SetMetricsSummary(context.Context, string, map[string]any) error {
	return nil
}
func (s *stubUserStore) GetUser(context.Context, string) (store.UserRecord, error) {
	if s.err != nil {
		return store.UserRecord{}, s.err
	}
	return s.user, nil
}

func TestLoadUserRecord_MissingRowNonProduction(t *testing.T) {
	t.Setenv("ENV", "development")

	h := NewHandler(nil, &stubUserStore{err: store.ErrUserNotFound}, nil, nil, nil, nil, nil, nil, nil)
	user, err := h.loadUserRecord(context.Background(), "dev-user")
	if err != nil {
		t.Fatalf("loadUserRecord: %v", err)
	}
	if store.ResolveEffectiveTier(user) != store.TierFree {
		t.Fatalf("tier: got %q", store.ResolveEffectiveTier(user))
	}
}

func TestLoadUserRecord_MissingRowProduction(t *testing.T) {
	t.Setenv("ENV", "production")

	h := NewHandler(nil, &stubUserStore{err: store.ErrUserNotFound}, nil, nil, nil, nil, nil, nil, nil)
	_, err := h.loadUserRecord(context.Background(), "user-1")
	if err == nil {
		t.Fatal("expected error in production when user row missing")
	}
}
