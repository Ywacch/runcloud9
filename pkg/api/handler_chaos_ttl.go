package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/ywacch/cloud9/pkg/chaos"
	"github.com/ywacch/cloud9/pkg/metrics"
)

const chaosPartitionSweepInterval = time.Minute

// StartChaosPartitionSweep re-schedules TTL timers on boot and expires overdue policies.
func (h *Handler) StartChaosPartitionSweep(ctx context.Context) {
	if h == nil || h.clientset == nil || h.store == nil {
		return
	}
	go h.chaosPartitionSweepLoop(ctx)
}

func (h *Handler) chaosPartitionSweepLoop(ctx context.Context) {
	h.recoverChaosPartitionTimers(ctx)

	ticker := time.NewTicker(chaosPartitionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.sweepExpiredChaosPartitions(ctx)
		}
	}
}

func (h *Handler) recoverChaosPartitionTimers(ctx context.Context) {
	rows, err := h.store.GetStaleDeployments(ctx)
	if err != nil {
		slog.Warn("chaos partition recovery: list deployments failed", "error", err)
		return
	}
	for _, row := range rows {
		h.refreshChaosPartitionTTL(ctx, row.Namespace)
	}
}

func (h *Handler) sweepExpiredChaosPartitions(ctx context.Context) {
	rows, err := h.store.GetStaleDeployments(ctx)
	if err != nil {
		slog.Warn("chaos partition sweep: list deployments failed", "error", err)
		return
	}
	now := time.Now()
	for _, row := range rows {
		healed, err := chaos.ExpireDuePartitions(ctx, h.clientset, row.Namespace, now)
		if err != nil {
			slog.Warn("chaos partition sweep failed", "namespace", row.Namespace, "error", err)
			continue
		}
		for _, part := range healed {
			h.cancelChaosPartitionTTL(row.Namespace)
			h.emitChaosHeal(row.Namespace, part, "expired")
		}
	}
}

func (h *Handler) refreshChaosPartitionTTL(ctx context.Context, namespace string) {
	if h == nil || h.clientset == nil {
		return
	}
	partitions, err := chaos.ListActivePartitions(ctx, h.clientset, namespace)
	if err != nil {
		slog.Warn("chaos partition ttl refresh failed", "namespace", namespace, "error", err)
		return
	}
	if len(partitions) == 0 {
		h.cancelChaosPartitionTTL(namespace)
		return
	}

	part := partitions[0]
	remaining := time.Until(part.ExpiresAt)
	if part.ExpiresAt.IsZero() || remaining <= 0 {
		h.fireChaosPartitionExpiry(namespace)
		return
	}

	h.chaosTTLMu.Lock()
	defer h.chaosTTLMu.Unlock()
	if h.chaosTTLTimers == nil {
		h.chaosTTLTimers = make(map[string]*time.Timer)
	}
	if old := h.chaosTTLTimers[namespace]; old != nil {
		old.Stop()
	}
	h.chaosTTLTimers[namespace] = time.AfterFunc(remaining, func() {
		h.fireChaosPartitionExpiry(namespace)
	})
}

func (h *Handler) cancelChaosPartitionTTL(namespace string) {
	if h == nil {
		return
	}
	h.chaosTTLMu.Lock()
	defer h.chaosTTLMu.Unlock()
	if h.chaosTTLTimers == nil {
		return
	}
	if timer := h.chaosTTLTimers[namespace]; timer != nil {
		timer.Stop()
	}
	delete(h.chaosTTLTimers, namespace)
}

func (h *Handler) fireChaosPartitionExpiry(namespace string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	healed, err := chaos.ExpireDuePartitions(ctx, h.clientset, namespace, time.Now())
	if err != nil {
		slog.Warn("chaos partition expiry failed", "namespace", namespace, "error", err)
		return
	}
	h.cancelChaosPartitionTTL(namespace)
	for _, part := range healed {
		h.emitChaosHeal(namespace, part, "expired")
	}
}

func (h *Handler) emitChaosHeal(namespace string, part chaos.ActivePartition, reason string) {
	if h == nil || h.eventBus == nil {
		return
	}
	h.eventBus.Emit(namespace, metrics.Event{
		Timestamp:   time.Now(),
		Type:        "action",
		Source:      "chaos",
		Component:   part.TargetComponent,
		Kind:        "chaos_heal",
		Audience:    "both",
		Title:       "Chaos partition healed",
		Description: part.Scenario,
		Detail: map[string]string{
			"scenario":        part.Scenario,
			"targetComponent": part.TargetComponent,
			"reason":          reason,
		},
	})
}
