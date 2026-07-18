package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

func TestHandleListDeploymentHistory_validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		store      deploymentStore
		userID     string
		query      string
		wantStatus int
	}{
		{
			name:       "store unavailable",
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "missing user",
			store:      newFakeDeploymentStore(),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "invalid limit",
			store:      newFakeDeploymentStore(),
			userID:     "user-1",
			query:      "?limit=zero",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-positive limit",
			store:      newFakeDeploymentStore(),
			userID:     "user-1",
			query:      "?limit=0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "store error",
			store: &fakeDeploymentStore{
				byNS:       make(map[string]store.DeploymentRow),
				historyErr: errors.New("query failed"),
			},
			userID:     "user-1",
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := NewHandler(nil, tc.store, nil, nil, nil, nil, nil, nil, nil)
			req := httptest.NewRequest(http.MethodGet, "/deployments/history"+tc.query, nil)
			if tc.userID != "" {
				req = req.WithContext(context.WithValue(req.Context(), userIDKey, tc.userID))
			}
			rec := httptest.NewRecorder()

			h.handleListDeploymentHistory(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestHandleListDeploymentHistory_success(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, time.July, 18, 10, 0, 0, 0, time.UTC)
	expiresAt := createdAt.Add(10 * time.Minute)
	fake := newFakeDeploymentStore()
	fake.history = []store.DeploymentHistoryRow{
		{
			ID:        "dep-1",
			Namespace: "cloud9-user-1",
			Template:  "url-shortener",
			Archetype: "browse-heavy",
			Status:    "terminated",
			MetricsSummary: map[string]any{
				"terminated_at":          "2026-07-18T10:09:00Z",
				"actual_duration_seconds": 540,
				"peak_rps":               1200.5,
				"peak_p99_ms":            json.Number("42.25"),
			},
			ExpiresAt: expiresAt,
			CreatedAt: createdAt,
		},
	}
	h := NewHandler(nil, fake, nil, nil, nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/deployments/history?limit=999", nil)
	req = req.WithContext(context.WithValue(req.Context(), userIDKey, "user-1"))
	rec := httptest.NewRecorder()

	h.handleListDeploymentHistory(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if fake.historyUser != "user-1" {
		t.Fatalf("store user = %q, want user-1", fake.historyUser)
	}
	if fake.historyLimit != maxDeploymentHistoryLimit {
		t.Fatalf("store limit = %d, want %d", fake.historyLimit, maxDeploymentHistoryLimit)
	}

	var got []deploymentHistoryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("response length = %d, want 1", len(got))
	}
	item := got[0]
	if item.ID != "dep-1" || item.Template != "url-shortener" || item.Archetype != "browse-heavy" {
		t.Fatalf("identity fields = %#v", item)
	}
	if item.TerminatedAt != "2026-07-18T10:09:00Z" {
		t.Fatalf("terminatedAt = %q", item.TerminatedAt)
	}
	if item.DurationSeconds == nil || *item.DurationSeconds != 540 {
		t.Fatalf("durationSeconds = %v", item.DurationSeconds)
	}
	if item.PeakRPS == nil || *item.PeakRPS != 1200.5 {
		t.Fatalf("peakRps = %v", item.PeakRPS)
	}
	if item.PeakP99Ms == nil || *item.PeakP99Ms != 42.25 {
		t.Fatalf("peakP99Ms = %v", item.PeakP99Ms)
	}
}
