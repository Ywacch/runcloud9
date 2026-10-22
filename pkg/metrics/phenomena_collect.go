package metrics

import (
	"fmt"
	"strings"

	"github.com/ywacch/cloud9/pkg/loadgen/vu"
	"github.com/ywacch/cloud9/pkg/phenomena"
)

func isSocialFeedTemplate(template string) bool {
	return strings.HasPrefix(template, "social-feed")
}

func appendReplyBeforeParentPhenomenon(dm *DeploymentMetrics, template string, topo TopologyFlags, traffic appTrafficConsistency) {
	if dm == nil || !isSocialFeedTemplate(template) || topo.DBReadReplicas < 1 || !traffic.ThreadCountersPresent {
		return
	}

	var obs vu.ObserverStats
	detectionIncomplete := false
	if dm.LoadGen != nil {
		detectionIncomplete = dm.LoadGen.DetectionMayBeIncomplete
		if dm.LoadGen.VU != nil {
			obs = dm.LoadGen.VU.Observer
		}
	}

	entry := phenomena.BuildReplyBeforeParent(phenomena.ReplyBeforeParentInput{
		AppReads:                 traffic.ThreadReadsTotal,
		AppOrphanReads:           traffic.ThreadOrphanReadsTotal,
		ObserverWindowReads:      obs.WindowReads,
		ObserverViolations:       obs.Violations,
		ObserverExamples:         observerExamplesToPhenomena(obs.Examples),
		DelaysMs:                 topo.DBReplicaApplyDelayMs,
		ReadConsistency:          topo.DBReadConsistency,
		ReadReplicas:             topo.DBReadReplicas,
		DetectionMayBeIncomplete: detectionIncomplete,
	})
	if entry != nil {
		dm.Phenomena = []phenomena.Entry{*entry}
	}
}

func observerExamplesToPhenomena(examples []vu.ObservationExample) []phenomena.Example {
	if len(examples) == 0 {
		return nil
	}
	out := make([]phenomena.Example, 0, len(examples))
	for _, ex := range examples {
		detail := map[string]string{
			"rootId":   fmt.Sprintf("%d", ex.RootID),
			"replyId":  fmt.Sprintf("%d", ex.ReplyID),
			"parentId": fmt.Sprintf("%d", ex.ParentID),
		}
		if ex.ServedBy.Replies != "" {
			detail["repliesHost"] = ex.ServedBy.Replies
		}
		if ex.ServedBy.Parents != "" {
			detail["parentsHost"] = ex.ServedBy.Parents
		}
		out = append(out, phenomena.Example{
			RequestID: ex.RequestID,
			At:        ex.At,
			Detail:    detail,
		})
	}
	return out
}
