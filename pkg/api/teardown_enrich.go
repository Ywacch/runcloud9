package api

import (
	"context"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
)

// EnrichTeardownSummary adds model reel captions to a teardown summary in place.
// It is a no-op when hist is empty or inference is off for the deployment owner.
func (h *Handler) EnrichTeardownSummary(
	ctx context.Context,
	namespace string,
	row store.DeploymentRow,
	summary map[string]any,
	hist []metrics.DeploymentMetrics,
	latest *metrics.DeploymentMetrics,
) {
	if h == nil || summary == nil || len(hist) == 0 {
		return
	}
	design, _ := deployer.DesignFromMap(row.Design)
	if design == nil {
		design = &deployer.DeploymentDesign{}
	}
	dc := deployer.DesignConfigFromDesign(design)
	tpl := row.Template
	enrichCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	h.enrichReelCaptionsInSummary(enrichCtx, namespace, row.UserID, summary, dc, tpl, hist, latest)
}
