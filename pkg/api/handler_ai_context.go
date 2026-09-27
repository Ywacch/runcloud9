package api

import (
	"encoding/json"
	"net/http"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
)

func (h *Handler) handleAIContext(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	row := h.requireOwnership(w, r, namespace)
	if row == nil {
		return
	}

	design, _ := deployer.DesignFromMap(row.Design)
	if design == nil {
		design = &deployer.DeploymentDesign{}
	}
	dc := deployer.DesignConfigFromDesign(design)

	var ctx AIContext
	var err error
	switch row.Status {
	case "terminated":
		ctx, err = aiContextFromSummary(namespace, row.Template, dc, design, row.MetricsSummary)
	default:
		ctx = h.aiContextFromLive(namespace, row.Template, dc, design)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ctx)
}

func (h *Handler) aiContextFromLive(namespace, template string, dc deployer.DesignConfig, design *deployer.DeploymentDesign) AIContext {
	var hist []metrics.DeploymentMetrics
	var latest *metrics.DeploymentMetrics
	if h.metricsCollector != nil {
		hist = h.metricsCollector.DownsampledSessionHistory(namespace)
		if len(hist) == 0 {
			hist = h.metricsCollector.SessionHistory(namespace)
		}
		latest = h.metricsCollector.Latest(namespace)
	}
	in := AIContextInputFromDeploymentDesign(namespace, template, dc, design, hist, finalSnapshot(hist, latest))
	return BuildAIContext(in)
}

func aiContextFromSummary(namespace, template string, dc deployer.DesignConfig, design *deployer.DeploymentDesign, summary map[string]any) (AIContext, error) {
	hist, err := SessionHistoryFromSummary(summary)
	if err != nil {
		return AIContext{}, err
	}
	in := AIContextInputFromDeploymentDesign(namespace, template, dc, design, hist, finalSnapshot(hist, nil))
	return BuildAIContext(in), nil
}
