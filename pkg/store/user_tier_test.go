package store

import (
	"testing"
	"time"
)

func TestResolveEffectiveTier(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(24 * time.Hour)
	past := time.Now().Add(-24 * time.Hour)

	tests := []struct {
		name string
		user UserRecord
		want string
	}{
		{
			name: "internal wins over premium",
			user: UserRecord{IsInternal: true, IsPremium: true, PremiumExpiresAt: &future},
			want: TierInternal,
		},
		{
			name: "active premium",
			user: UserRecord{IsPremium: true, PremiumExpiresAt: &future},
			want: TierEngineer,
		},
		{
			name: "expired premium",
			user: UserRecord{IsPremium: true, PremiumExpiresAt: &past},
			want: TierFree,
		},
		{
			name: "premium without expiry",
			user: UserRecord{IsPremium: true},
			want: TierFree,
		},
		{name: "default free", user: UserRecord{}, want: TierFree},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ResolveEffectiveTier(tt.user); got != tt.want {
				t.Fatalf("ResolveEffectiveTier() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetTierLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tier                string
		wantConcurrent      int
		wantSessionCap      int
		wantMonthlySessions int
		wantAllTemplates    bool
		wantMaxAppReplicas  int32
		wantAllowLB         bool
		wantAllowMQWorker   bool
		wantAllowProxyCache bool
		wantAllowAutoscale  bool
		wantMaxInstanceSize string
		wantMaxLoadgenRPS   int
		wantAllowEngineSwap bool
		wantAllowHeavy      bool
	}{
		{
			tier: TierFree, wantConcurrent: 1, wantSessionCap: 9, wantMonthlySessions: 3, wantAllTemplates: false,
			wantMaxAppReplicas: 5, wantAllowLB: true, wantAllowMQWorker: true, wantAllowProxyCache: true,
			wantAllowAutoscale:  false,
			wantMaxInstanceSize: InstanceSizeMedium, wantMaxLoadgenRPS: LoadgenRPSCapFree,
			wantAllowEngineSwap: false, wantAllowHeavy: false,
		},
		{
			tier: TierEngineer, wantConcurrent: 1, wantSessionCap: 15, wantMonthlySessions: 20, wantAllTemplates: true,
			wantMaxAppReplicas: 5, wantAllowLB: true, wantAllowMQWorker: true, wantAllowProxyCache: true,
			wantAllowAutoscale:  false,
			wantMaxInstanceSize: InstanceSizeLarge, wantMaxLoadgenRPS: LoadgenRPSCapEngineer,
			wantAllowEngineSwap: true, wantAllowHeavy: false,
		},
		{
			tier: TierInternal, wantConcurrent: 40, wantSessionCap: 30, wantMonthlySessions: 0, wantAllTemplates: true,
			wantMaxAppReplicas: 5, wantAllowLB: true, wantAllowMQWorker: true, wantAllowProxyCache: true,
			wantAllowAutoscale:  true,
			wantMaxInstanceSize: InstanceSizeLarge, wantMaxLoadgenRPS: LoadgenRPSCapInternal,
			wantAllowEngineSwap: true, wantAllowHeavy: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.tier, func(t *testing.T) {
			t.Parallel()
			got := GetTierLimits(tt.tier)
			if got.MaxConcurrent != tt.wantConcurrent {
				t.Fatalf("MaxConcurrent = %d, want %d", got.MaxConcurrent, tt.wantConcurrent)
			}
			if got.SessionCapMins != tt.wantSessionCap {
				t.Fatalf("SessionCapMins = %d, want %d", got.SessionCapMins, tt.wantSessionCap)
			}
			if got.MaxSessionsPerMonth != tt.wantMonthlySessions {
				t.Fatalf("MaxSessionsPerMonth = %d, want %d", got.MaxSessionsPerMonth, tt.wantMonthlySessions)
			}
			if got.AllTemplates != tt.wantAllTemplates {
				t.Fatalf("AllTemplates = %v, want %v", got.AllTemplates, tt.wantAllTemplates)
			}
			if got.MaxAppServerReplicas != tt.wantMaxAppReplicas {
				t.Fatalf("MaxAppServerReplicas = %d, want %d", got.MaxAppServerReplicas, tt.wantMaxAppReplicas)
			}
			if got.AllowLoadBalancer != tt.wantAllowLB {
				t.Fatalf("AllowLoadBalancer = %v, want %v", got.AllowLoadBalancer, tt.wantAllowLB)
			}
			if got.AllowMQWorker != tt.wantAllowMQWorker {
				t.Fatalf("AllowMQWorker = %v, want %v", got.AllowMQWorker, tt.wantAllowMQWorker)
			}
			if got.AllowProxyCache != tt.wantAllowProxyCache {
				t.Fatalf("AllowProxyCache = %v, want %v", got.AllowProxyCache, tt.wantAllowProxyCache)
			}
			if got.AllowAutoscale != tt.wantAllowAutoscale {
				t.Fatalf("AllowAutoscale = %v, want %v", got.AllowAutoscale, tt.wantAllowAutoscale)
			}
			if got.MaxInstanceSize != tt.wantMaxInstanceSize {
				t.Fatalf("MaxInstanceSize = %q, want %q", got.MaxInstanceSize, tt.wantMaxInstanceSize)
			}
			if got.MaxLoadgenRPS != tt.wantMaxLoadgenRPS {
				t.Fatalf("MaxLoadgenRPS = %d, want %d", got.MaxLoadgenRPS, tt.wantMaxLoadgenRPS)
			}
			if got.AllowEngineSwap != tt.wantAllowEngineSwap {
				t.Fatalf("AllowEngineSwap = %v, want %v", got.AllowEngineSwap, tt.wantAllowEngineSwap)
			}
			if got.AllowHeavyEngines != tt.wantAllowHeavy {
				t.Fatalf("AllowHeavyEngines = %v, want %v", got.AllowHeavyEngines, tt.wantAllowHeavy)
			}
			topo := got.TopologyLimits()
			if topo.AllowProxyCache != tt.wantAllowProxyCache {
				t.Fatalf("TopologyLimits().AllowProxyCache = %v, want %v", topo.AllowProxyCache, tt.wantAllowProxyCache)
			}
			if topo.AllowAutoscale != tt.wantAllowAutoscale {
				t.Fatalf("TopologyLimits().AllowAutoscale = %v, want %v", topo.AllowAutoscale, tt.wantAllowAutoscale)
			}
		})
	}
}

func TestResolveSessionMinutes(t *testing.T) {
	t.Parallel()

	req12 := 12
	req30 := 30

	tests := []struct {
		name      string
		tier      string
		requested *int
		want      int
	}{
		{name: "free ignores request", tier: TierFree, requested: &req30, want: 9},
		{name: "engineer default", tier: TierEngineer, requested: nil, want: 9},
		{name: "engineer requested", tier: TierEngineer, requested: &req12, want: 12},
		{name: "engineer clamps high", tier: TierEngineer, requested: &req30, want: 15},
		{name: "engineer clamps low", tier: TierEngineer, requested: intPtr(3), want: 9},
		{name: "internal caps at 30", tier: TierInternal, requested: intPtr(45), want: 30},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ResolveSessionMinutes(tt.tier, tt.requested); got != tt.want {
				t.Fatalf("ResolveSessionMinutes() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestResourceQuotaTier(t *testing.T) {
	t.Parallel()

	if got := ResourceQuotaTier(TierFree); got != "explorer" {
		t.Fatalf("free: got %q", got)
	}
	if got := ResourceQuotaTier(TierEngineer); got != TierEngineer {
		t.Fatalf("engineer: got %q", got)
	}
	if got := ResourceQuotaTier(TierInternal); got != TierInternal {
		t.Fatalf("internal: got %q", got)
	}
}

func TestStartOfUTCMonth(t *testing.T) {
	t.Parallel()
	got := StartOfUTCMonth(time.Date(2026, 7, 22, 15, 30, 0, 0, time.UTC))
	want := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("StartOfUTCMonth = %v, want %v", got, want)
	}
}

func intPtr(v int) *int { return &v }
