package loadgen

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/loadgen/vu"
	"github.com/ywacch/cloud9/pkg/templates"
)

// PodStartBody is the JSON body POSTed to the in-namespace loadgen pod.
type PodStartBody struct {
	RPS             int              `json:"rps"`
	DurationSeconds int64            `json:"durationSeconds"`
	ScheduleID      string           `json:"scheduleId,omitempty"`
	Phases          []Phase          `json:"phases,omitempty"`
	Targets         []PodStartTarget `json:"targets,omitempty"`
	Actor           *PodActorConfig  `json:"actor,omitempty"`
	VU              *PodVUConfig     `json:"vu,omitempty"`
	Distribution    map[string]int   `json:"distribution,omitempty"`
	ActiveUsers     int              `json:"activeUsers,omitempty"`
}

// PodStartTarget is one pre-resolved static target for the loadgen pod.
type PodStartTarget struct {
	URL    string `json:"url"`
	Method string `json:"method"`
	Body   string `json:"body"`
	Weight int    `json:"weight"`
	Key    string `json:"key,omitempty"`
}

// BuildPodStartBody resolves a profile into the pod start protocol (static or actor mode).
func BuildPodStartBody(ctx context.Context, namespace string, profile LoadProfile) (PodStartBody, error) {
	plan, err := profileToTargetPlan(ctx, namespace, profile)
	if err != nil {
		return PodStartBody{}, err
	}

	body := PodStartBody{
		RPS:             profile.RPS,
		DurationSeconds: profile.DurationSeconds,
		ScheduleID:      profile.ScheduleID,
		Phases:          append([]Phase(nil), profile.Phases...),
		Distribution:    plan.Distribution,
		ActiveUsers:     profile.ActiveUsers,
	}

	if plan.Actor != nil {
		specs, ok := cloneEndpointSpecs(profile.Template)
		if !ok {
			return PodStartBody{}, fmt.Errorf("loadgen: unknown template %q", profile.Template)
		}
		paramDefs, _ := actorParamsFromTemplate(profile.Template)
		baseURL := resolveLoadgenBaseURL(namespace, profile)
		body.Actor = &PodActorConfig{
			BaseURL:      baseURL,
			Endpoints:    specs,
			Distribution: plan.Distribution,
			Pools:        capIdentityPools(flatPoolsFromSpecs(ctx, baseURL, specs), profile.ActiveUsers),
			SegmentPools: capIdentityPools(actorSegmentPoolsFromTemplate(ctx, baseURL, profile.Template), profile.ActiveUsers),
			ActiveUsers:  profile.ActiveUsers,
			ParamDefs:    paramDefs,
			ParamValues:  profile.ActorParams,
		}
		if err := fillPodVU(ctx, namespace, profile, &body); err != nil {
			return PodStartBody{}, err
		}
		return body, nil
	}

	body.Targets = make([]PodStartTarget, 0, len(plan.Static))
	for _, t := range plan.Static {
		body.Targets = append(body.Targets, PodStartTarget{
			URL:    t.URL,
			Method: t.Method,
			Body:   t.Body,
			Weight: t.Weight,
			Key:    t.Key,
		})
	}
	if err := fillPodVU(ctx, namespace, profile, &body); err != nil {
		return PodStartBody{}, err
	}
	return body, nil
}

func fillPodVU(ctx context.Context, namespace string, profile LoadProfile, body *PodStartBody) error {
	if profile.VU == nil {
		return nil
	}
	spec, ok := templates.LoadgenVU(profile.Template)
	if !ok || spec == nil {
		return fmt.Errorf("loadgen: template %q has no vu config", profile.Template)
	}
	baseURL := resolveLoadgenBaseURL(namespace, profile)
	pool := fetchPool(ctx, baseURL, spec.Identity.PoolEndpoint, spec.Identity.PoolField)
	pool = capIdentitySlice(pool, profile.ActiveUsers)
	if len(pool) == 0 {
		return fmt.Errorf("loadgen: vu identity pool is empty")
	}
	actions, ok := VUActionsFromTemplate(profile.Template)
	if !ok {
		actions = map[string]vu.Action{}
	}
	body.VU = &PodVUConfig{
		Config:  *profile.VU,
		BaseURL: baseURL,
		Actions: actions,
		UserIDs: pool,
	}
	return nil
}
