package loadgen

import (
	"context"
	"fmt"
)

// PodStartBody is the JSON body POSTed to the in-namespace loadgen pod.
type PodStartBody struct {
	RPS             int              `json:"rps"`
	DurationSeconds int64            `json:"durationSeconds"`
	ScheduleID      string           `json:"scheduleId,omitempty"`
	Phases          []Phase          `json:"phases,omitempty"`
	Targets         []PodStartTarget `json:"targets,omitempty"`
	Actor           *PodActorConfig  `json:"actor,omitempty"`
	Distribution    map[string]int   `json:"distribution,omitempty"`
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
			Pools:        flatPoolsFromSpecs(ctx, baseURL, specs),
			SegmentPools: actorSegmentPoolsFromTemplate(ctx, baseURL, profile.Template),
			ParamDefs:    paramDefs,
			ParamValues:  profile.ActorParams,
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
	return body, nil
}
