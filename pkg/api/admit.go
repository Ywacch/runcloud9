package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ywacch/cloud9/pkg/admission"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/store"
)

const (
	clusterBusyRetryAfterSeconds = 45
	queueFullRetryAfterSeconds   = 45

	statusProvisioning = "provisioning"
	statusQueued       = "queued"
)

var (
	errUserAtCapacity = errors.New("user at concurrent deployment capacity")
	// errDuplicateCreate is returned when a non-burst create follows another active
	// deployment created within duplicateCreateWindow on the same API replica.
	errDuplicateCreate      = errors.New("duplicate create within cooldown")
	errMonthlySessionLimit  = errors.New("monthly session limit reached")
	errClusterBusy          = errors.New("cluster deploy budget full")
	errQueueFull            = errors.New("deploy queue full")
	errDesignExceedsCluster = errors.New("design exceeds cluster capacity")
	errListActive           = errors.New("list active deployments")
	errSumOccupancy         = errors.New("sum admission occupancy")
	errCountQueued          = errors.New("count queued deployments")
	errCountMonthlySessions = errors.New("count monthly sessions")
	errDesignEncode         = errors.New("encode design")
	errCreateRow            = errors.New("create deployment row")
)

const (
	designExceedsClusterMessage = "this design needs more CPU or memory than the cluster provides"
	monthlySessionLimitMessage  = "monthly session limit reached — upgrade or wait until next month"
	userAtCapacityMessage       = "you already have an active deployment — end it before creating another"

	duplicateCreateWindow = 3 * time.Second
)

// hasRecentActiveCreate reports whether any active row was created within window of now.
// Rows with zero CreatedAt are treated as not recent (e.g. test fixtures).
func hasRecentActiveCreate(active []store.ActiveDeploymentRow, now time.Time, window time.Duration) bool {
	for _, row := range active {
		if row.CreatedAt.IsZero() {
			continue
		}
		if now.Sub(row.CreatedAt) < window {
			return true
		}
	}
	return false
}

// admitAndCreate enforces per-user concurrency, a short duplicate-create cooldown
// (unless allowBurst), monthly session caps, and optional room budget, then inserts
// the deployment row as provisioning or queued.
// Caller must hold h.admitMu for the duration on a single-replica API.
// The duplicate-create check is best-effort on one replica; burst callers opt out
// via allowBurst (X-Cloud9-Batch-Launch).
//
// Returns the persisted status ("provisioning" or "queued").
func (h *Handler) admitAndCreate(
	ctx context.Context,
	userID string,
	maxConcurrent int,
	maxSessionsPerMonth int,
	design deployer.DeploymentDesign,
	resourceQuotaTier string,
	namespace string,
	sessionExpiresAt time.Time,
	allowBurst bool,
) (string, error) {
	if h.store == nil {
		return statusProvisioning, nil
	}

	active, err := h.store.ListActiveDeployments(ctx, userID)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errListActive, err)
	}
	if !allowBurst && hasRecentActiveCreate(active, time.Now(), duplicateCreateWindow) {
		return "", errDuplicateCreate
	}
	if len(active) >= maxConcurrent {
		return "", errUserAtCapacity
	}

	if maxSessionsPerMonth > 0 {
		n, err := h.store.CountUserDeploymentsSince(ctx, userID, store.StartOfUTCMonth(time.Now()))
		if err != nil {
			return "", fmt.Errorf("%w: %w", errCountMonthlySessions, err)
		}
		if n >= maxSessionsPerMonth {
			return "", errMonthlySessionLimit
		}
	}

	weight := admission.WeightFromBudget(deployer.QuotaResourceLimits(design, resourceQuotaTier))

	status := statusProvisioning
	expiresAt := sessionExpiresAt
	queuedAhead := 0

	if h.roomBudget != nil {
		if admission.ExceedsRoom(h.roomBudget, weight) {
			return "", errDesignExceedsCluster
		}
		occRow, err := h.store.SumActiveAdmissionWeights(ctx)
		if err != nil {
			return "", fmt.Errorf("%w: %w", errSumOccupancy, err)
		}
		occ := admission.Occupancy{
			CPUMillicores: occRow.CPUMillicores,
			MemoryMi:      occRow.MemoryMi,
		}
		if !admission.Fits(h.roomBudget, occ, weight) {
			n, err := h.store.CountQueuedDeployments(ctx)
			if err != nil {
				return "", fmt.Errorf("%w: %w", errCountQueued, err)
			}
			if n >= h.maxDeployQueue {
				h.recordAdmissionEvent(ctx, store.AdmissionEvent{
					UserID:        userID,
					Template:      design.Template,
					Kind:          store.AdmissionKindQueueFull,
					QueuePosition: intPtr(n),
					Paid:          h.admissionPaid(ctx, userID),
				})
				return "", errQueueFull
			}
			status = statusQueued
			expiresAt = time.Now().Add(h.queueTTL)
			queuedAhead = n
		}
	}

	designMap, err := designToMap(design)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errDesignEncode, err)
	}

	_, err = h.store.CreateDeployment(ctx, store.CreateDeploymentParams{
		Namespace:     namespace,
		Template:      design.Template,
		Archetype:     design.Archetype,
		Design:        designMap,
		ExpiresAt:     expiresAt,
		UserID:        userID,
		AdmittedCPUm:  weight.CPUMillicores,
		AdmittedMemMi: weight.MemoryMi,
		Status:        status,
	})
	if err != nil {
		return "", fmt.Errorf("%w: %w", errCreateRow, err)
	}
	h.stampRunManifest(ctx, namespace, design)
	if status == statusQueued {
		now := time.Now()
		h.stampMetrics(ctx, namespace, map[string]any{
			"queued_at": rfc3339UTC(now),
		})
		h.recordAdmissionEvent(ctx, store.AdmissionEvent{
			UserID:        userID,
			Namespace:     namespace,
			Template:      design.Template,
			Kind:          store.AdmissionKindRoomQueued,
			QueuePosition: intPtr(queuedAhead + 1),
			Paid:          h.admissionPaid(ctx, userID),
		})
	}
	return status, nil
}

func designToMap(design deployer.DeploymentDesign) (map[string]any, error) {
	b, err := json.Marshal(design)
	if err != nil {
		return nil, err
	}
	var designMap map[string]any
	if err := json.Unmarshal(b, &designMap); err != nil {
		return nil, err
	}
	return designMap, nil
}

func writeClusterBusy(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", fmt.Sprintf("%d", clusterBusyRetryAfterSeconds))
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":             "cluster is at capacity — try again shortly",
		"reason":            "cluster_busy",
		"retryAfterSeconds": clusterBusyRetryAfterSeconds,
	})
}

func writeQueueFull(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", fmt.Sprintf("%d", queueFullRetryAfterSeconds))
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":             "deploy queue is full — try again shortly",
		"reason":            "queue_full",
		"retryAfterSeconds": queueFullRetryAfterSeconds,
	})
}

func writeDesignExceedsCluster(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnprocessableEntity)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":  designExceedsClusterMessage,
		"reason": "design_exceeds_cluster_capacity",
	})
}

func writeMonthlySessionLimit(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":  monthlySessionLimitMessage,
		"reason": "monthly_session_limit",
	})
}

func writeAdmitError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errUserAtCapacity):
		writeError(w, http.StatusTooManyRequests, userAtCapacityMessage)
	case errors.Is(err, errDuplicateCreate):
		writeErrorWithReason(w, http.StatusTooManyRequests, userAtCapacityMessage, "duplicate_create")
	case errors.Is(err, errMonthlySessionLimit):
		writeMonthlySessionLimit(w)
	case errors.Is(err, errDesignExceedsCluster):
		writeDesignExceedsCluster(w)
	case errors.Is(err, errQueueFull):
		writeQueueFull(w)
	case errors.Is(err, errClusterBusy):
		writeClusterBusy(w)
	case errors.Is(err, errListActive):
		writeError(w, http.StatusInternalServerError, "failed to check active deployments")
	case errors.Is(err, errSumOccupancy):
		writeError(w, http.StatusInternalServerError, "failed to check cluster capacity")
	case errors.Is(err, errCountQueued):
		writeError(w, http.StatusInternalServerError, "failed to check deploy queue")
	case errors.Is(err, errCountMonthlySessions):
		writeError(w, http.StatusInternalServerError, "failed to check monthly session quota")
	case errors.Is(err, errDesignEncode):
		writeError(w, http.StatusInternalServerError, "invalid design")
	default:
		writeError(w, http.StatusInternalServerError, "failed to create deployment record")
	}
}
