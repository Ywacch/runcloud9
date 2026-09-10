package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

func TestHandleGetCurrentUser_Unauthorized(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "test-secret")
	t.Setenv("SUPABASE_URL", "https://example.supabase.co")
	t.Setenv("ENV", "production")

	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleGetCurrentUser(t *testing.T) {
	future := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	futureRFC := future.Format(time.RFC3339)

	tests := []struct {
		name              string
		env               string
		user              store.UserRecord
		userErr           error
		wantTier          string
		wantQuota         string
		wantStatus        int
		wantLB            bool
		wantMaxRep        int32
		wantMaxSz         string
		wantMaxLoadgenRps int
	}{
		{
			name:              "free tier dev default",
			env:               "development",
			wantTier:          store.TierFree,
			wantQuota:         "explorer",
			wantStatus:        http.StatusOK,
			wantLB:            true,
			wantMaxRep:        3,
			wantMaxSz:         store.InstanceSizeMedium,
			wantMaxLoadgenRps: store.LoadgenRPSCapFree,
		},
		{
			name: "engineer tier",
			user: store.UserRecord{
				IsPremium:        true,
				PremiumExpiresAt: &future,
			},
			wantTier:          store.TierEngineer,
			wantQuota:         store.TierEngineer,
			wantStatus:        http.StatusOK,
			wantLB:            true,
			wantMaxRep:        3,
			wantMaxSz:         store.InstanceSizeLarge,
			wantMaxLoadgenRps: store.LoadgenRPSCapEngineer,
		},
		{
			name: "internal tier",
			user: store.UserRecord{
				IsInternal: true,
			},
			wantTier:          store.TierInternal,
			wantQuota:         store.TierInternal,
			wantStatus:        http.StatusOK,
			wantLB:            true,
			wantMaxRep:        3,
			wantMaxSz:         store.InstanceSizeLarge,
			wantMaxLoadgenRps: store.LoadgenRPSCapInternal,
		},
		{
			name:       "missing user production",
			env:        "production",
			userErr:    store.ErrUserNotFound,
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ENV", tt.env)
			t.Setenv("SUPABASE_JWT_SECRET", "")
			t.Setenv("SUPABASE_URL", "")

			s := &stubUserStore{user: tt.user, err: tt.userErr, monthlyUsed: 2}
			h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
			mux := http.NewServeMux()
			h.RegisterRoutes(mux)

			req := httptest.NewRequest(http.MethodGet, "/me", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status: want %d, got %d body=%s", tt.wantStatus, rec.Code, rec.Body.String())
			}
			if tt.wantStatus != http.StatusOK {
				return
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type: want application/json, got %q", ct)
			}

			var out currentUserResponse
			if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
				t.Fatal(err)
			}
			if out.UserID != "dev-user" {
				t.Fatalf("userId: got %q want dev-user", out.UserID)
			}
			if out.EffectiveTier != tt.wantTier {
				t.Fatalf("effectiveTier: got %q want %q", out.EffectiveTier, tt.wantTier)
			}
			if out.QuotaTier != tt.wantQuota {
				t.Fatalf("quotaTier: got %q want %q", out.QuotaTier, tt.wantQuota)
			}
			if out.Limits.AllowLoadBalancer != tt.wantLB {
				t.Fatalf("allowLoadBalancer: got %v want %v", out.Limits.AllowLoadBalancer, tt.wantLB)
			}
			if out.Limits.MaxAppServerReplicas != tt.wantMaxRep {
				t.Fatalf("maxAppServerReplicas: got %d want %d", out.Limits.MaxAppServerReplicas, tt.wantMaxRep)
			}
			if out.Limits.MaxInstanceSize != tt.wantMaxSz {
				t.Fatalf("maxInstanceSize: got %q want %q", out.Limits.MaxInstanceSize, tt.wantMaxSz)
			}
			if out.Limits.MaxLoadgenRps != tt.wantMaxLoadgenRps {
				t.Fatalf("maxLoadgenRps: got %d want %d", out.Limits.MaxLoadgenRps, tt.wantMaxLoadgenRps)
			}
			switch tt.wantTier {
			case store.TierFree:
				if out.Limits.MaxSessionsPerMonth == nil || *out.Limits.MaxSessionsPerMonth != 3 {
					t.Fatalf("maxSessionsPerMonth: got %v want 3", out.Limits.MaxSessionsPerMonth)
				}
				if out.SessionsUsedThisMonth != 2 {
					t.Fatalf("sessionsUsedThisMonth: got %d want 2", out.SessionsUsedThisMonth)
				}
				if out.SessionsRemainingThisMonth == nil || *out.SessionsRemainingThisMonth != 1 {
					t.Fatalf("sessionsRemainingThisMonth: got %v want 1", out.SessionsRemainingThisMonth)
				}
			case store.TierEngineer:
				if out.Limits.MaxSessionsPerMonth == nil || *out.Limits.MaxSessionsPerMonth != 20 {
					t.Fatalf("maxSessionsPerMonth: got %v want 20", out.Limits.MaxSessionsPerMonth)
				}
				if out.SessionsUsedThisMonth != 2 {
					t.Fatalf("sessionsUsedThisMonth: got %d want 2", out.SessionsUsedThisMonth)
				}
				if out.SessionsRemainingThisMonth == nil || *out.SessionsRemainingThisMonth != 18 {
					t.Fatalf("sessionsRemainingThisMonth: got %v want 18", out.SessionsRemainingThisMonth)
				}
			case store.TierInternal:
				if out.Limits.MaxSessionsPerMonth != nil {
					t.Fatalf("maxSessionsPerMonth: got %v want null", out.Limits.MaxSessionsPerMonth)
				}
				if out.SessionsRemainingThisMonth != nil {
					t.Fatalf("sessionsRemainingThisMonth: got %v want null", out.SessionsRemainingThisMonth)
				}
			}
			if tt.user.IsPremium {
				if out.PremiumExpiresAt == nil || *out.PremiumExpiresAt != futureRFC {
					t.Fatalf("premiumExpiresAt: got %v want %q", out.PremiumExpiresAt, futureRFC)
				}
			}
		})
	}
}

func TestBuildCurrentUserResponse_PremiumExpired(t *testing.T) {
	t.Parallel()

	past := time.Now().Add(-time.Hour)
	resp := buildCurrentUserResponse("user-1", store.UserRecord{
		IsPremium:        true,
		PremiumExpiresAt: &past,
	}, 0)
	if resp.EffectiveTier != store.TierFree {
		t.Fatalf("effectiveTier: got %q want %q", resp.EffectiveTier, store.TierFree)
	}
	if resp.QuotaTier != "explorer" {
		t.Fatalf("quotaTier: got %q want explorer", resp.QuotaTier)
	}
}
