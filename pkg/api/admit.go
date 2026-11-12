package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
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
	statusRunning      = "running"
	statusFailed       = "failed"
	statusTerminating  = "terminating"
	statusTerminated   = "terminated"
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

// admitQueueFullError is errQueueFull plus the queue depth at reject time.
// The handler records the admission event after it releases admitMu.
type admitQueueFullError struct {
	depth int
}

func (e *admitQueueFullError) Error() string {
	if e == nil {
		return errQueueFull.Error()
	}
	return errQueueFull.Error()
}

func (e *admitQueueFullError) Unwrap() error { return errQueueFull }

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
// Caller must hold h.admitMu for this call on a single-replica API.
// Do not call finishAdmit while that lock is held.
// The duplicate-create check is best-effort on one replica; burst callers opt out
// via allowBurst (X-Cloud9-Batch-Launch).
//
// Returns the persisted status ("provisioning" or "queued") and how many queued
// rows were ahead of this one when status is queued.
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
) (string, int, error) {
	if h.store == nil {
		return statusProvisioning, 0, nil
	}

	var err error
	defer func() {
		if err != nil {
			h.observeAdmitResult(ctx, "", err)
		}
	}()

	snap, err := h.loadAdmitSnapshot(ctx, userID, maxSessionsPerMonth)
	if err != nil {
		return "", 0, err
	}
	if !allowBurst && hasRecentActiveCreate(snap.active, time.Now(), duplicateCreateWindow) {
		err = errDuplicateCreate
		return "", 0, err
	}
	if len(snap.active) >= maxConcurrent {
		err = errUserAtCapacity
		return "", 0, err
	}
	if maxSessionsPerMonth > 0 && snap.monthly >= maxSessionsPerMonth {
		err = errMonthlySessionLimit
		return "", 0, err
	}

	weight := admission.WeightFromBudget(deployer.QuotaResourceLimits(design, resourceQuotaTier))

	status := statusProvisioning
	expiresAt := sessionExpiresAt
	queuedAhead := 0

	if h.roomBudget != nil {
		if admission.ExceedsRoom(h.roomBudget, weight) {
			err = errDesignExceedsCluster
			return "", 0, err
		}
		if !admission.Fits(h.roomBudget, snap.occ, weight) {
			n, countErr := h.store.CountQueuedDeployments(ctx)
			if countErr != nil {
				err = fmt.Errorf("%w: %w", errCountQueued, countErr)
				return "", 0, err
			}
			if n >= h.maxDeployQueue {
				err = &admitQueueFullError{depth: n}
				return "", 0, err
			}
			status = statusQueued
			expiresAt = time.Now().Add(h.queueTTL)
			queuedAhead = n
		}
	}

	designMap, mapErr := designToMap(design)
	if mapErr != nil {
		err = fmt.Errorf("%w: %w", errDesignEncode, mapErr)
		return "", 0, err
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
		err = fmt.Errorf("%w: %w", errCreateRow, err)
		return "", 0, err
	}
	return status, queuedAhead, nil
}

// finishAdmit writes the run manifest and admission events after admitMu is released
// and after the HTTP response headers are sent.
// ctx is detached from client cancel so a client that already read the response
// does not abort the manifest write.
func (h *Handler) finishAdmit(ctx context.Context, userID string, design deployer.DeploymentDesign, namespace, status string, queuedAhead int) {
	if h == nil || h.store == nil || namespace == "" {
		return
	}
	ctx = context.WithoutCancel(ctx)
	h.stampRunManifest(ctx, namespace, design)
	if status == statusQueued {
		h.stampMetrics(ctx, namespace, map[string]any{
			"queued_at": rfc3339UTC(time.Now()),
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
	h.observeAdmitResult(ctx, status, nil)
}

type admitSnapshot struct {
	active  []store.ActiveDeploymentRow
	occ     admission.Occupancy
	monthly int
}

// admissionOccupancySource is implemented by the session registry decorator.
type admissionOccupancySource interface {
	AdmissionOccupancy() store.AdmissionOccupancy
}

// loadAdmitSnapshot reads the rows the admit decision needs.
// Independent reads run together. The caller still holds admitMu, so the
// following insert stays ordered with other admits.
func (h *Handler) loadAdmitSnapshot(ctx context.Context, userID string, maxSessionsPerMonth int) (admitSnapshot, error) {
	var (
		snap     admitSnapshot
		listErr  error
		sumErr   error
		monthErr error
		wg       sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		snap.active, listErr = h.store.ListActiveDeployments(ctx, userID)
	}()
	if h.roomBudget != nil {
		if occSrc, ok := h.store.(admissionOccupancySource); ok {
			row := occSrc.AdmissionOccupancy()
			snap.occ = admission.Occupancy{
				CPUMillicores: row.CPUMillicores,
				MemoryMi:      row.MemoryMi,
			}
		} else {
			wg.Add(1)
			go func() {
				defer wg.Done()
				row, err := h.store.SumActiveAdmissionWeights(ctx)
				sumErr = err
				snap.occ = admission.Occupancy{
					CPUMillicores: row.CPUMillicores,
					MemoryMi:      row.MemoryMi,
				}
			}()
		}
	}
	if maxSessionsPerMonth > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap.monthly, monthErr = h.store.CountUserDeploymentsSince(ctx, userID, store.StartOfUTCMonth(time.Now()))
		}()
	}
	wg.Wait()
	if listErr != nil {
		return admitSnapshot{}, fmt.Errorf("%w: %w", errListActive, listErr)
	}
	if sumErr != nil {
		return admitSnapshot{}, fmt.Errorf("%w: %w", errSumOccupancy, sumErr)
	}
	if monthErr != nil {
		return admitSnapshot{}, fmt.Errorf("%w: %w", errCountMonthlySessions, monthErr)
	}
	return snap, nil
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
