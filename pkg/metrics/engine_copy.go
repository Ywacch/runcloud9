package metrics

import (
	"strings"

	"github.com/ywacch/cloud9/pkg/engines"
)

// WorkQueueEngineLabel returns the catalog display label for the deployment's work queue engine.
func WorkQueueEngineLabel(dm *DeploymentMetrics) string {
	if dm != nil && dm.Placement != nil {
		if j, ok := dm.Placement.JobByID("work_queue"); ok {
			id := strings.ToLower(strings.TrimSpace(j.Engine))
			if id != "" {
				if eng, ok := engines.Default().EngineByID(id); ok && strings.TrimSpace(eng.Label) != "" {
					return eng.Label
				}
				return id
			}
		}
	}
	if eng, ok := engines.Default().EngineByID("rabbitmq"); ok && strings.TrimSpace(eng.Label) != "" {
		return eng.Label
	}
	return "message queue"
}
