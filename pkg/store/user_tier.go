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
	AllTemplates         bool
	MaxAppServerReplicas int32
	AllowLoadBalancer    bool
	AllowMQWorker        bool
	AllowProxyCache      bool
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
			MaxConcurrent:        40,
			SessionDefaultMins:   9,
			SessionCapMins:       30,
			AllTemplates:         true,
			MaxAppServerReplicas: 3,
			AllowLoadBalancer:    true,
			AllowMQWorker:        true,
			AllowProxyCache:      true,
		}
	case TierEngineer:
		return TierLimits{
			MaxConcurrent:        1,
			SessionDefaultMins:   9,
			SessionCapMins:       15,
			AllTemplates:         true,
			MaxAppServerReplicas: 3,
			AllowLoadBalancer:    true,
			AllowMQWorker:        true,
			AllowProxyCache:      true,
		}
	default:
		return TierLimits{
			MaxConcurrent:        1,
			SessionDefaultMins:   9,
			SessionCapMins:       9,
			AllTemplates:         false,
			MaxAppServerReplicas: 1,
			AllowLoadBalancer:    false,
			AllowMQWorker:        false,
			AllowProxyCache:      false,
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
