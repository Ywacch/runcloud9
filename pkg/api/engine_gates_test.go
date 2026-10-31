package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestBuildEngineGates(t *testing.T) {
	t.Parallel()
	cat := engines.Default()

	tests := []struct {
		name     string
		limits   store.TierLimits
		job      string
		engine   string
		wantGate EnginePairGate
	}{
		{
			name:     "internal allows timeline on postgres",
			limits:   store.GetTierLimits(store.TierInternal),
			job:      "timeline",
			engine:   "postgresql",
			wantGate: EnginePairGateNone,
		},
		{
			name:     "free gates non-default timeline postgres",
			limits:   store.GetTierLimits(store.TierFree),
			job:      "timeline",
			engine:   "postgresql",
			wantGate: EnginePairGateTier,
		},
		{
			name:     "free allows default timeline redis",
			limits:   store.GetTierLimits(store.TierFree),
			job:      "timeline",
			engine:   "redis",
			wantGate: EnginePairGateNone,
		},
		{
			name:     "engineer allows timeline postgres",
			limits:   store.GetTierLimits(store.TierEngineer),
			job:      "timeline",
			engine:   "postgresql",
			wantGate: EnginePairGateNone,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := buildEngineGates(tc.limits, cat)
			var gate EnginePairGate
			for _, e := range got {
				if e.Job == tc.job && e.Engine == tc.engine {
					gate = e.Gate
					break
				}
			}
			if gate != tc.wantGate {
				t.Fatalf("gate for %s/%s = %q want %q", tc.job, tc.engine, gate, tc.wantGate)
			}
		})
	}
}

func TestHandleEngineGates(t *testing.T) {
	cat := engines.Default()
	future := time.Now().Add(24 * time.Hour)

	tests := []struct {
		name     string
		user     store.UserRecord
		job      string
		engine   string
		wantGate EnginePairGate
	}{
		{
			name:     "free tier gates timeline postgres",
			user:     store.UserRecord{},
			job:      "timeline",
			engine:   "postgresql",
			wantGate: EnginePairGateTier,
		},
		{
			name: "engineer tier allows swap",
			user: store.UserRecord{
				IsPremium:        true,
				PremiumExpiresAt: &future,
			},
			job:      "timeline",
			engine:   "postgresql",
			wantGate: EnginePairGateNone,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ENV", "development")
			s := newFakeDeploymentStore()
			s.users = map[string]store.UserRecord{"dev-user": tc.user}
			h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
			mux := http.NewServeMux()
			h.RegisterRoutes(mux)

			req := httptest.NewRequest(http.MethodGet, "/me/engine-gates", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status: %d body=%s", rec.Code, rec.Body.String())
			}
			var resp engineGatesResponse
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatal(err)
			}
			if len(resp.Pairs) != len(cat.Pairs) {
				t.Fatalf("pairs: got %d want %d", len(resp.Pairs), len(cat.Pairs))
			}
			var gate EnginePairGate
			for _, e := range resp.Pairs {
				if e.Job == tc.job && e.Engine == tc.engine {
					gate = e.Gate
					break
				}
			}
			if gate != tc.wantGate {
				t.Fatalf("gate for %s/%s = %q want %q", tc.job, tc.engine, gate, tc.wantGate)
			}
		})
	}
}
