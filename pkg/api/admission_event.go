package api

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

func intPtr(n int) *int {
	return &n
}

func durationMillis(d time.Duration) int {
	if d < 0 {
		return 0
	}
	ms := d / time.Millisecond
	if ms > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(ms)
}

func rfc3339UTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func (h *Handler) admissionPaid(ctx context.Context, userID string) bool {
	if h == nil || h.store == nil || userID == "" {
		return false
	}
	user, err := h.store.GetUser(ctx, userID)
	if err != nil {
		return false
	}
	return provisionIsPaid(user)
}

// recordAdmissionEvent writes one wait/reject row. Errors are logged; callers
// must not fail admit or provision because this insert failed.
func (h *Handler) recordAdmissionEvent(ctx context.Context, ev store.AdmissionEvent) {
	if h == nil || h.store == nil {
		return
	}
	if err := h.store.InsertAdmissionEvent(ctx, ev); err != nil {
		log.Printf("admission event kind=%s namespace=%s: %v", ev.Kind, ev.Namespace, err)
	}
}

func (h *Handler) stampMetrics(ctx context.Context, namespace string, patch map[string]any) {
	if h == nil || h.store == nil || namespace == "" || len(patch) == 0 {
		return
	}
	h.emitProvisionMu.Lock()
	defer h.emitProvisionMu.Unlock()
	merged := make(map[string]any, len(patch))
	if row, err := h.store.GetDeployment(ctx, namespace); err == nil && row.MetricsSummary != nil {
		merged = make(map[string]any, len(row.MetricsSummary)+len(patch))
		for k, v := range row.MetricsSummary {
			merged[k] = v
		}
	}
	for k, v := range patch {
		merged[k] = v
	}
	if err := h.store.UpdateMetricsSummary(ctx, namespace, merged); err != nil {
		log.Printf("admission stamp metrics namespace=%s: %v", namespace, err)
	}
}
