package api

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/admission"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/store"
)

const (
	defaultMaxDeployQueue      = 20
	defaultQueueTTLSeconds     = 900
	defaultProvisionETASeconds = 240
	queueTickInterval          = 15 * time.Second
)

// provisionETA tracks a rolling average of provision wall times for queue wait estimates.
type provisionETA struct {
	mu              sync.Mutex
	avgSeconds      float64
	fallbackSeconds float64
	samples         int
}

func newProvisionETA(fallbackSeconds float64) *provisionETA {
	if fallbackSeconds <= 0 {
		fallbackSeconds = float64(defaultProvisionETASeconds)
	}
	return &provisionETA{fallbackSeconds: fallbackSeconds}
}

func (e *provisionETA) Observe(d time.Duration) {
	if e == nil || d <= 0 {
		return
	}
	sec := d.Seconds()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.samples == 0 {
		e.avgSeconds = sec
	} else {
		// EWMA: recent finishes move the estimate without thrashing on outliers.
		e.avgSeconds = 0.3*sec + 0.7*e.avgSeconds
	}
	e.samples++
}

func (e *provisionETA) Seconds() float64 {
	if e == nil {
		return float64(defaultProvisionETASeconds)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.samples == 0 {
		return e.fallbackSeconds
	}
	return e.avgSeconds
}

func queueConfigFromEnv() (maxQueue int, ttl time.Duration, etaSeconds float64) {
	maxQueue = defaultMaxDeployQueue
	if raw := strings.TrimSpace(os.Getenv("MAX_DEPLOY_QUEUE")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			maxQueue = n
		} else {
			log.Printf("MAX_DEPLOY_QUEUE invalid %q (using %d)", raw, defaultMaxDeployQueue)
		}
	}

	ttlSec := defaultQueueTTLSeconds
	if raw := strings.TrimSpace(os.Getenv("DEPLOY_QUEUE_TTL_SECONDS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			ttlSec = n
		} else {
			log.Printf("DEPLOY_QUEUE_TTL_SECONDS invalid %q (using %d)", raw, defaultQueueTTLSeconds)
		}
	}

	etaSeconds = float64(defaultProvisionETASeconds)
	if raw := strings.TrimSpace(os.Getenv("PROVISION_ETA_SECONDS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			etaSeconds = float64(n)
		} else {
			log.Printf("PROVISION_ETA_SECONDS invalid %q (using %d)", raw, defaultProvisionETASeconds)
		}
	}

	return maxQueue, time.Duration(ttlSec) * time.Second, etaSeconds
}

// StartDeployQueue runs the first-fit promote/expire loop until ctx is cancelled.
// Safe to call once after NewHandler; no-op when store is nil.
func (h *Handler) StartDeployQueue(ctx context.Context) {
	if h == nil || h.store == nil {
		return
	}
	go h.deployQueueLoop(ctx)
}

func (h *Handler) wakeDeployQueue() {
	if h == nil || h.queueWake == nil {
		return
	}
	select {
	case h.queueWake <- struct{}{}:
	default:
	}
}

func (h *Handler) deployQueueLoop(ctx context.Context) {
	ticker := time.NewTicker(queueTickInterval)
	defer ticker.Stop()

	h.drainDeployQueue(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.queueWake:
			h.drainDeployQueue(ctx)
		case <-ticker.C:
			h.drainDeployQueue(ctx)
		}
	}
}

// drainDeployQueue expires TTL'd entries, clears poison (ExceedsRoom) heads, and
// promotes with first-fit over one queued snapshot (skips blocked heads).
func (h *Handler) drainDeployQueue(ctx context.Context) {
	if h.store == nil {
		return
	}

	h.admitMu.Lock()
	defer h.admitMu.Unlock()

	queued, err := h.store.ListQueuedDeployments(ctx)
	if err != nil {
		log.Printf("deploy queue: list queued: %v", err)
		return
	}
	defer h.refreshDeployQueueDepth(ctx)
	if len(queued) == 0 {
		return
	}

	for _, row := range queued {
		if !row.ExpiresAt.IsZero() && time.Now().After(row.ExpiresAt) {
			h.terminateQueuedLocked(ctx, row.Namespace, "queue_ttl", true)
			continue
		}

		if h.roomBudget != nil {
			weight := admission.Weight{
				CPUMillicores: derefInt64(row.AdmittedCPUm),
				MemoryMi:      derefInt64(row.AdmittedMemMi),
			}
			if admission.ExceedsRoom(h.roomBudget, weight) {
				h.terminateQueuedLocked(ctx, row.Namespace, "design_exceeds_cluster_capacity", false)
			}
		}
	}

	if h.roomBudget == nil {
		h.promoteQueuedPass(ctx, queued, nil, true)
		h.promoteQueuedPass(ctx, queued, nil, false)
		return
	}

	var occ admission.Occupancy
	occRow, err := h.store.SumActiveAdmissionWeights(ctx)
	if err != nil {
		log.Printf("deploy queue: sum occupancy: %v", err)
		return
	}
	occ = admission.Occupancy{
		CPUMillicores: occRow.CPUMillicores,
		MemoryMi:      occRow.MemoryMi,
	}
	occ = h.promoteQueuedPass(ctx, queued, &occ, true)
	h.promoteQueuedPass(ctx, queued, &occ, false)
}

func (h *Handler) queueRowIsPaid(ctx context.Context, row store.QueuedDeploymentRow) bool {
	if row.UserID == "" {
		return false
	}
	user, err := h.store.GetUser(ctx, row.UserID)
	if err != nil {
		return false
	}
	return provisionIsPaid(user)
}

// promoteQueuedPass promotes queued rows for one lane (paid or explorer) with first-fit packing.
func (h *Handler) promoteQueuedPass(
	ctx context.Context,
	queued []store.QueuedDeploymentRow,
	occ *admission.Occupancy,
	paidLane bool,
) admission.Occupancy {
	var out admission.Occupancy
	if occ != nil {
		out = *occ
	}
	promoted := make(map[string]struct{})

	for _, row := range queued {
		isPaid := h.queueRowIsPaid(ctx, row)
		if isPaid != paidLane {
			continue
		}
		if _, done := promoted[row.Namespace]; done {
			continue
		}

		if h.roomBudget != nil {
			weight := admission.Weight{
				CPUMillicores: derefInt64(row.AdmittedCPUm),
				MemoryMi:      derefInt64(row.AdmittedMemMi),
			}
			if !admission.Fits(h.roomBudget, out, weight) {
				continue
			}
			if !h.promoteQueuedLocked(ctx, row) {
				return out
			}
			out.CPUMillicores += weight.CPUMillicores
			out.MemoryMi += weight.MemoryMi
			promoted[row.Namespace] = struct{}{}
			continue
		}

		if !h.promoteQueuedLocked(ctx, row) {
			return out
		}
		promoted[row.Namespace] = struct{}{}
	}
	return out
}

func (h *Handler) terminateQueuedLocked(ctx context.Context, namespace, reason string, queueExpired bool) {
	log.Printf("deploy queue: terminating queued namespace=%s reason=%s", namespace, reason)
	row, rowErr := h.store.GetDeployment(ctx, namespace)
	_ = h.store.UpdateStatus(ctx, namespace, "terminated")
	now := time.Now()
	summary := map[string]any{
		"terminated_at": rfc3339UTC(now),
		"reason":        reason,
	}
	if queueExpired {
		summary["queue_expired"] = true
	}
	_ = h.store.UpdateMetricsSummary(ctx, namespace, summary)
	if queueExpired && rowErr == nil {
		h.recordAdmissionEvent(ctx, store.AdmissionEvent{
			UserID:    row.UserID,
			Namespace: namespace,
			Template:  row.Template,
			Kind:      store.AdmissionKindRoomExpired,
			WaitMS:    intPtr(durationMillis(now.Sub(row.CreatedAt))),
			Paid:      h.admissionPaid(ctx, row.UserID),
			Detail:    map[string]any{"reason": reason},
		})
	}
}

// promoteQueuedLocked claims the head and starts background provision.
// Returns false when claim failed or design could not be loaded (caller should stop).
func (h *Handler) promoteQueuedLocked(ctx context.Context, head store.QueuedDeploymentRow) bool {
	claimed, err := h.store.ClaimQueued(ctx, head.Namespace)
	if err != nil {
		log.Printf("deploy queue: claim %s: %v", head.Namespace, err)
		return false
	}
	if !claimed {
		return false
	}

	promotedAt := time.Now()
	h.stampMetrics(ctx, head.Namespace, map[string]any{
		"promoted_at": rfc3339UTC(promotedAt),
	})
	h.recordAdmissionEvent(ctx, store.AdmissionEvent{
		UserID:    head.UserID,
		Namespace: head.Namespace,
		Template:  head.Template,
		Kind:      store.AdmissionKindRoomPromoted,
		WaitMS:    intPtr(durationMillis(promotedAt.Sub(head.CreatedAt))),
		Paid:      h.queueRowIsPaid(ctx, head),
	})

	row, err := h.store.GetDeployment(ctx, head.Namespace)
	if err != nil {
		log.Printf("deploy queue: get after claim %s: %v", head.Namespace, err)
		_ = h.store.UpdateStatus(ctx, head.Namespace, "failed")
		return false
	}

	designPtr, err := deployer.DesignFromMap(row.Design)
	if err != nil || designPtr == nil {
		log.Printf("deploy queue: design %s: %v", head.Namespace, err)
		_ = h.store.UpdateStatus(ctx, head.Namespace, "failed")
		return false
	}
	design := *designPtr
	deployer.NormalizeDesign(&design)

	userID := row.UserID
	quotaTier := "explorer"
	sessionMins := 9
	if userID != "" {
		if userRecord, uerr := h.store.GetUser(ctx, userID); uerr == nil {
			effective := store.ResolveEffectiveTier(userRecord)
			quotaTier = store.ResourceQuotaTier(effective)
			sessionMins = store.ResolveSessionMinutes(effective, design.SessionMinutes)
		}
	}
	runExpiresAt := time.Now().Add(time.Duration(sessionMins) * time.Minute)
	_ = h.store.UpdateExpiresAt(ctx, head.Namespace, runExpiresAt)

	log.Printf("deploy queue: promoting namespace=%s to provisioning", head.Namespace)
	go h.runBackgroundProvision(design, head.Namespace, userID, quotaTier, sessionMins)
	return true
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// queuePositionAndETA returns 1-based FIFO position and estimated wait for a queued namespace.
func (h *Handler) queuePositionAndETA(ctx context.Context, namespace string) (position int, etaSec int, ok bool) {
	if h.store == nil {
		return 0, 0, false
	}
	queued, err := h.store.ListQueuedDeployments(ctx)
	if err != nil {
		return 0, 0, false
	}
	for i, q := range queued {
		if q.Namespace == namespace {
			pos := i + 1
			avg := h.provisionETA.Seconds()
			return pos, int(float64(pos) * avg), true
		}
	}
	return 0, 0, false
}
