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

// StartDeployQueue runs the FIFO promote/expire loop until ctx is cancelled.
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

// drainDeployQueue expires TTL'd heads and promotes as many consecutive
// head-of-line entries as currently fit (strict FIFO: stop on first that does not).
func (h *Handler) drainDeployQueue(ctx context.Context) {
	if h.store == nil {
		return
	}

	h.admitMu.Lock()
	defer h.admitMu.Unlock()

	for {
		queued, err := h.store.ListQueuedDeployments(ctx)
		if err != nil {
			log.Printf("deploy queue: list queued: %v", err)
			return
		}
		if len(queued) == 0 {
			return
		}

		head := queued[0]
		if !head.ExpiresAt.IsZero() && time.Now().After(head.ExpiresAt) {
			h.expireQueuedLocked(ctx, head.Namespace)
			continue
		}

		if h.roomBudget == nil {
			// Room gate off: promote head immediately (should be rare for queued rows).
			if !h.promoteQueuedLocked(ctx, head) {
				return
			}
			continue
		}

		weight := admission.Weight{
			CPUMillicores: derefInt64(head.AdmittedCPUm),
			MemoryMi:      derefInt64(head.AdmittedMemMi),
		}
		occRow, err := h.store.SumActiveAdmissionWeights(ctx)
		if err != nil {
			log.Printf("deploy queue: sum occupancy: %v", err)
			return
		}
		occ := admission.Occupancy{
			CPUMillicores: occRow.CPUMillicores,
			MemoryMi:      occRow.MemoryMi,
		}
		if !admission.Fits(h.roomBudget, occ, weight) {
			return
		}
		if !h.promoteQueuedLocked(ctx, head) {
			return
		}
	}
}

func (h *Handler) expireQueuedLocked(ctx context.Context, namespace string) {
	log.Printf("deploy queue: expiring queued namespace=%s", namespace)
	_ = h.store.UpdateStatus(ctx, namespace, "terminated")
	_ = h.store.UpdateMetricsSummary(ctx, namespace, map[string]any{
		"queue_expired": true,
		"terminated_at": time.Now().UTC().Format(time.RFC3339),
		"reason":        "queue_ttl",
	})
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
