package api

import (
	"encoding/json"
	"net/http"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
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
		ctx, err = aiContextFromSummary(row, dc, design)
	default:
		ctx = h.aiContextFromLive(row, dc, design)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ctx)
}

func deploymentRunRole(row *store.DeploymentRow) string {
	if row == nil || row.RunRole == nil {
		return ""
	}
	return *row.RunRole
}

func (h *Handler) aiContextFromLive(row *store.DeploymentRow, dc deployer.DesignConfig, design *deployer.DeploymentDesign) AIContext {
	namespace := row.Namespace
	template := row.Template
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
	in.RunRole = deploymentRunRole(row)
	return BuildAIContext(in)
}

func aiContextFromSummary(row *store.DeploymentRow, dc deployer.DesignConfig, design *deployer.DeploymentDesign) (AIContext, error) {
	hist, err := SessionHistoryFromSummary(row.MetricsSummary)
	if err != nil {
		return AIContext{}, err
	}
	traceSamples, err := metrics.TraceSamplesFromSummary(row.MetricsSummary)
	if err != nil {
		return AIContext{}, err
	}
	in := AIContextInputFromDeploymentDesign(row.Namespace, row.Template, dc, design, hist, finalSnapshot(hist, nil))
	in.RunRole = deploymentRunRole(row)
	in.TraceSamples = traceSamples
	return BuildAIContext(in), nil
}
