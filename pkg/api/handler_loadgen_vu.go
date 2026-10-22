package api

import (
	"strconv"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/loadgen/vu"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/store"
)

func (h *Handler) applyVUObserverMaxThreadAge(profile loadgen.LoadProfile, row *store.DeploymentRow) loadgen.LoadProfile {
	if profile.VU == nil {
		return profile
	}
	hasObserver := false
	for _, b := range profile.VU.Behaviors {
		if b.Name == "thread_observer" {
			hasObserver = true
			break
		}
	}
	if !hasObserver {
		return profile
	}
	stagger := 0
	if row != nil {
		design, err := deployer.DesignFromMap(row.Design)
		if err == nil && design != nil {
			dbNode := firstDBNode(design)
			if dbNode != nil {
				readReplicas := int(deployer.ParseReadReplicaCount(dbNode.Config))
				if readReplicas >= 1 {
					delays, err := deployer.ParseReplicaApplyDelayMs(dbNode.Config, readReplicas)
					if err == nil {
						stagger = vu.StaggerMs(delays)
					}
				}
			}
		}
	}
	cfg := vu.ApplyObserverStaggerMaxAge(*profile.VU, stagger)
	profile.VU = &cfg
	return profile
}

func (h *Handler) emitVUGapWarningIfNeeded(namespace string, profile loadgen.LoadProfile, row *store.DeploymentRow) {
	if h.eventBus == nil || profile.VU == nil || row == nil {
		return
	}
	design, err := deployer.DesignFromMap(row.Design)
	if err != nil || design == nil {
		return
	}
	dbNode := firstDBNode(design)
	if dbNode == nil {
		return
	}
	readReplicas := int(deployer.ParseReadReplicaCount(dbNode.Config))
	if readReplicas < 1 {
		return
	}
	delays, err := deployer.ParseReplicaApplyDelayMs(dbNode.Config, readReplicas)
	if err != nil {
		return
	}
	msg := vu.ConfigReplyGapWarning(*profile.VU, delays)
	if msg == "" {
		return
	}
	replyWithin := vu.ParseReplierReplyWithinMs(*profile.VU)
	h.eventBus.Emit(namespace, metrics.Event{
		Timestamp:   time.Now(),
		Type:        "workload",
		Source:      "loadgen",
		Kind:        "vu_gap_warning",
		Visibility:  "all",
		Title:       "Reply gap warning",
		Description: msg,
		Detail: map[string]string{
			"fidelity":      rules.FidelitySimulated,
			"replyWithinMs": strconv.Itoa(replyWithin),
			"staggerMs":     strconv.Itoa(vu.StaggerMs(delays)),
			"message":       msg,
		},
	})
}
