package templates

import (
	"fmt"
	"slices"

	"github.com/ywacch/cloud9/pkg/engines"
)

// JobRequirement declares a platform job a template may bind and which engines are allowed.
type JobRequirement struct {
	ID              string   `json:"id" yaml:"id"`
	Required        bool     `json:"required" yaml:"required"`
	AllowedEngines  []string `json:"allowedEngines" yaml:"allowedEngines"`
	DefaultNodeType string   `json:"defaultNodeType,omitempty" yaml:"defaultNodeType,omitempty"`
	Variations      []string `json:"variations,omitempty" yaml:"variations,omitempty"`
}

func resolveJobRequirements(def *templateDef, cat *engines.Catalog) ([]JobRequirement, error) {
	if def == nil {
		return nil, fmt.Errorf("nil template def")
	}
	if cat == nil {
		return nil, fmt.Errorf("nil engines catalog")
	}

	reqs := append([]JobRequirement(nil), def.Jobs...)
	if def.WorkerTimelineStore == "redis" && !jobRequirementContains(reqs, "timeline") {
		reqs = append(reqs, JobRequirement{
			ID:              "timeline",
			Required:        true,
			AllowedEngines:  []string{"redis"},
			DefaultNodeType: "cache",
		})
	}

	if err := validateJobRequirements(def.ID, reqs, cat); err != nil {
		return nil, err
	}
	out := make([]JobRequirement, len(reqs))
	copy(out, reqs)
	return out, nil
}

func validateJobRequirements(templateID string, reqs []JobRequirement, cat *engines.Catalog) error {
	seen := make(map[string]struct{}, len(reqs))
	byComponent := make(map[string]string)
	for _, req := range reqs {
		if req.ID == "" {
			return fmt.Errorf("template %q: job requirement missing id", templateID)
		}
		if _, dup := seen[req.ID]; dup {
			return fmt.Errorf("template %q: duplicate job id %q", templateID, req.ID)
		}
		seen[req.ID] = struct{}{}

		if !catalogHasJob(cat, req.ID) {
			return fmt.Errorf("template %q: unknown job id %q", templateID, req.ID)
		}
		job, ok := cat.JobByID(req.ID)
		if !ok {
			return fmt.Errorf("template %q: unknown job id %q", templateID, req.ID)
		}
		if other, dup := byComponent[job.Component]; dup {
			return fmt.Errorf("template %q: jobs %q and %q share component %q", templateID, other, req.ID, job.Component)
		}
		byComponent[job.Component] = req.ID
		if len(req.AllowedEngines) == 0 {
			return fmt.Errorf("template %q: job %q: allowedEngines is required", templateID, req.ID)
		}
		switch req.DefaultNodeType {
		case "", "db", "cache", "mq":
		default:
			return fmt.Errorf("template %q: job %q: bad defaultNodeType %q", templateID, req.ID, req.DefaultNodeType)
		}

		engineSeen := make(map[string]struct{}, len(req.AllowedEngines))
		for _, eng := range req.AllowedEngines {
			if eng == "" {
				return fmt.Errorf("template %q: job %q: empty engine id", templateID, req.ID)
			}
			if _, dup := engineSeen[eng]; dup {
				return fmt.Errorf("template %q: job %q: duplicate allowed engine %q", templateID, req.ID, eng)
			}
			engineSeen[eng] = struct{}{}
			if !catalogHasEngine(cat, eng) {
				return fmt.Errorf("template %q: job %q: unknown engine id %q", templateID, req.ID, eng)
			}
			pair, ok := cat.Pair(req.ID, eng)
			if !ok {
				return fmt.Errorf("template %q: job %q: no catalog pair for engine %q", templateID, req.ID, eng)
			}
			if pair.Build != engines.BuildAvailable {
				return fmt.Errorf("template %q: job %q: engine %q is not build-available", templateID, req.ID, eng)
			}
		}
	}
	return nil
}

func jobRequirementContains(reqs []JobRequirement, id string) bool {
	return slices.ContainsFunc(reqs, func(r JobRequirement) bool { return r.ID == id })
}

func catalogHasJob(cat *engines.Catalog, id string) bool {
	for _, j := range cat.Jobs {
		if j.ID == id {
			return true
		}
	}
	return false
}

func catalogHasEngine(cat *engines.Catalog, id string) bool {
	for _, e := range cat.Engines {
		if e.ID == id {
			return true
		}
	}
	return false
}
