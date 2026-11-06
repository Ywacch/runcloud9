package store

import (
	"errors"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
)

const (
	TierFree     = "free"
	TierEngineer = "engineer"
	TierInternal = "internal"

	// Live loadgen RPS ceilings per effective tier (measured on url-shortener).
	LoadgenRPSCapFree     = 500
	LoadgenRPSCapEngineer = 1000
	LoadgenRPSCapInternal = 2000

	// Instance size ceilings per tier (deployer normalizes alpha → small).
	InstanceSizeMedium = "medium"
	InstanceSizeLarge  = "large"
)

// ErrUserNotFound is returned when no public.users row exists for the ID.
var ErrUserNotFound = errors.New("user not found")

// UserRecord holds subscription and admin fields from public.users.
type UserRecord struct {
	IsInternal         bool
	IsPremium          bool
	PremiumExpiresAt   *time.Time
	SubscriptionTier   string
	SubscriptionStatus string
}

// TierLimits are deployment policy caps for an effective tier.
type TierLimits struct {
	MaxConcurrent        int
	SessionDefaultMins   int
	SessionCapMins       int
	MaxSessionsPerMonth  int // 0 = unlimited
	AllTemplates         bool
	MaxAppServerReplicas int32
	AllowLoadBalancer    bool
	AllowMQWorker        bool
	AllowProxyCache      bool
	AllowAutoscale       bool   // CPU HPA on app-server and worker; internal only for now
	MaxInstanceSize      string // small/medium/large ceiling for canvas sizing knobs
	MaxLoadgenRPS        int    // live loadgen requests-per-second ceiling
	AllowEngineSwap      bool   // non-default job-engine pairs in the picker
	AllowHeavyEngines    bool   // heavy-weight engines from the catalog
}

// StartOfUTCMonth returns 00:00:00 UTC on the first day of t's calendar month.
func StartOfUTCMonth(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// ResolveEffectiveTier maps a user record to free, engineer, or internal.
func ResolveEffectiveTier(user UserRecord) string {
	if user.IsInternal {
		return TierInternal
	}
	if user.IsPremium && user.PremiumExpiresAt != nil && time.Now().Before(*user.PremiumExpiresAt) {
		return TierEngineer
	}
	return TierFree
}

// GetTierLimits returns deployment caps for an effective tier name.
func GetTierLimits(tier string) TierLimits {
	switch strings.ToLower(strings.TrimSpace(tier)) {
	case TierInternal:
		return TierLimits{
			// Raised for capacity-measurement campaigns (bouncer Phase 0).
			// This is an authorization ceiling, not a cluster admission budget.
			// Must cover rung-1 (50 sessions in one wave): cmd/capacity-probe/testdata/waves/rung-1.yaml
			MaxConcurrent:        50,
			SessionDefaultMins:   9,
			SessionCapMins:       30,
			MaxSessionsPerMonth:  0, // unlimited
			AllTemplates:         true,
			MaxAppServerReplicas: 5,
			AllowLoadBalancer:    true,
			AllowMQWorker:        true,
			AllowProxyCache:      true,
			AllowAutoscale:       true,
			MaxInstanceSize:      InstanceSizeLarge,
			MaxLoadgenRPS:        LoadgenRPSCapInternal,
			AllowEngineSwap:      true,
			AllowHeavyEngines:    true,
		}
	case TierEngineer:
		return TierLimits{
			MaxConcurrent:        1,
			SessionDefaultMins:   9,
			SessionCapMins:       15,
			MaxSessionsPerMonth:  20,
			AllTemplates:         true,
			MaxAppServerReplicas: 5,
			AllowLoadBalancer:    true,
			AllowMQWorker:        true,
			AllowProxyCache:      true,
			MaxInstanceSize:      InstanceSizeLarge,
			MaxLoadgenRPS:        LoadgenRPSCapEngineer,
			AllowEngineSwap:      true,
			AllowHeavyEngines:    false,
		}
	default:
		return TierLimits{
			MaxConcurrent:        1,
			SessionDefaultMins:   9,
			SessionCapMins:       9,
			MaxSessionsPerMonth:  3,
			AllTemplates:         false,
			MaxAppServerReplicas: 5,
			AllowLoadBalancer:    true,
			AllowMQWorker:        true,
			AllowProxyCache:      true,
			MaxInstanceSize:      InstanceSizeMedium,
			MaxLoadgenRPS:        LoadgenRPSCapFree,
			AllowEngineSwap:      false,
			AllowHeavyEngines:    false,
		}
	}
}

// ResolveSessionMinutes picks session length from tier rules and an optional request value.
func ResolveSessionMinutes(tier string, requested *int) int {
	limits := GetTierLimits(tier)
	if strings.ToLower(strings.TrimSpace(tier)) == TierFree {
		return limits.SessionCapMins
	}
	minutes := limits.SessionDefaultMins
	if requested != nil {
		minutes = *requested
	}
	if minutes < limits.SessionDefaultMins {
		minutes = limits.SessionDefaultMins
	}
	if minutes > limits.SessionCapMins {
		minutes = limits.SessionCapMins
	}
	return minutes
}

// TopologyLimits maps store tier limits to deployer topology validation input.
func (l TierLimits) TopologyLimits() deployer.TierTopologyLimits {
	return deployer.TierTopologyLimits{
		MaxAppServerReplicas: l.MaxAppServerReplicas,
		AllowLoadBalancer:    l.AllowLoadBalancer,
		AllowMQWorker:        l.AllowMQWorker,
		AllowProxyCache:      l.AllowProxyCache,
		AllowAutoscale:       l.AllowAutoscale,
	}
}

// ResourceQuotaTier maps effective tier to deployer quota tier names.
func ResourceQuotaTier(effectiveTier string) string {
	switch strings.ToLower(strings.TrimSpace(effectiveTier)) {
	case TierEngineer:
		return TierEngineer
	case TierInternal:
		return TierInternal
	default:
		return "explorer"
	}
}
