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
	errClusterBusy    = errors.New("cluster deploy budget full")
	errQueueFull      = errors.New("deploy queue full")
	errListActive     = errors.New("list active deployments")
	errSumOccupancy   = errors.New("sum admission occupancy")
	errCountQueued    = errors.New("count queued deployments")
	errDesignEncode   = errors.New("encode design")
	errCreateRow      = errors.New("create deployment row")
)

// admitAndCreate enforces per-user concurrency and optional room budget, then
// inserts the deployment row as provisioning or queued. Caller must hold
// h.admitMu for the duration on a single-replica API.
//
// Returns the persisted status ("provisioning" or "queued").
func (h *Handler) admitAndCreate(
	ctx context.Context,
	userID string,
	maxConcurrent int,
	design deployer.DeploymentDesign,
	resourceQuotaTier string,
	namespace string,
	sessionExpiresAt time.Time,
) (string, error) {
	if h.store == nil {
		return statusProvisioning, nil
	}

	active, err := h.store.ListActiveDeployments(ctx, userID)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errListActive, err)
	}
	if len(active) >= maxConcurrent {
		return "", errUserAtCapacity
	}

	weight := admission.WeightFromBudget(deployer.QuotaResourceLimits(design, resourceQuotaTier))

	status := statusProvisioning
	expiresAt := sessionExpiresAt

	if h.roomBudget != nil {
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
				return "", errQueueFull
			}
			status = statusQueued
			expiresAt = time.Now().Add(h.queueTTL)
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

func writeAdmitError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errUserAtCapacity):
		writeError(w, http.StatusTooManyRequests, "you already have an active deployment — end it before creating another")
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
	case errors.Is(err, errDesignEncode):
		writeError(w, http.StatusInternalServerError, "invalid design")
	default:
		writeError(w, http.StatusInternalServerError, "failed to create deployment record")
	}
}
