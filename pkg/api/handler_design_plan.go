package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/designplan"
	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/store"
	"github.com/ywacch/cloud9/pkg/templates"
)

type designPlanRequest struct {
	Base   *deployer.DeploymentDesign `json:"base,omitempty"`
	Design deployer.DeploymentDesign  `json:"design"`
}

func (h *Handler) handleDesignPlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	limitJSONBody(w, r)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req designPlanRequest
	if err := dec.Decode(&req); err != nil {
		if isBodyTooLarge(err) {
			writeError(w, http.StatusBadRequest, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	proposed := req.Design
	deployer.NormalizeDesign(&proposed)

	tmpl := templates.GetTemplate(proposed.Template)
	if tmpl == nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("template %q is not registered", proposed.Template))
		return
	}

	userID := userIDFromContext(r.Context())
	userRecord, err := h.loadUserRecord(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load user")
		return
	}
	tier := store.ResourceQuotaTier(store.ResolveEffectiveTier(userRecord))

	base := deployer.DeploymentDesign{}
	if req.Base != nil {
		base = *req.Base
	}

	plan, err := designplan.Build(designplan.Input{
		Base:      base,
		Proposed:  proposed,
		Template:  tmpl,
		Catalog:   engines.Default(),
		QuotaTier: tier,
		Room:      h.roomBudget,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(plan)
}
