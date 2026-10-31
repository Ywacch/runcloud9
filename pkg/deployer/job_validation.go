package deployer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/templates"
)

var (
	// ErrJobUnbound means a required job has no resolved binding on the design.
	ErrJobUnbound = errors.New("job unbound")
	// ErrEngineNotAllowed means the bound engine is not listed on the template job.
	ErrEngineNotAllowed = errors.New("engine not allowed for job")
	// ErrImpossiblePair means the catalog marks the job-engine pair impossible.
	ErrImpossiblePair = errors.New("engine cannot do this job")
	// ErrBindingNoNode means jobBindings references a node id that is not on the design.
	ErrBindingNoNode = errors.New("job bound to a missing node")
)

// JobBindingError carries job binding validation details for API 400 responses.
type JobBindingError struct {
	Job, Engine, NodeID string
	Reason              string // pair reason for ErrImpossiblePair; allowed engine CSV for unbound/not allowed
	Err                 error  // one of the sentinels above
}

func (e *JobBindingError) Error() string {
	if e == nil {
		return ""
	}
	switch e.Err {
	case ErrJobUnbound:
		return jobUnboundMessage(e.Job, e.Reason)
	case ErrEngineNotAllowed:
		return fmt.Sprintf(
			"%s cannot use %s on this template: allowed engines are %s",
			e.Job, e.Engine, strings.TrimSpace(e.Reason),
		)
	case ErrImpossiblePair:
		if e.Reason != "" {
			return fmt.Sprintf("%s cannot use %s: %s", e.Job, e.Engine, e.Reason)
		}
		return fmt.Sprintf("%s cannot use %s", e.Job, e.Engine)
	case ErrBindingNoNode:
		return fmt.Sprintf("%s job is bound to missing node %q", e.Job, e.NodeID)
	default:
		return fmt.Sprintf("job binding %q: %v", e.Job, e.Err)
	}
}

func (e *JobBindingError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ValidateJobBindings checks resolved job bindings against the template and engines catalog.
func ValidateJobBindings(design DeploymentDesign, tmpl *templates.TemplateSpec, cat *engines.Catalog) error {
	if tmpl == nil || len(tmpl.Jobs) == 0 {
		return nil
	}
	if cat == nil {
		cat = engines.Default()
	}

	bindings := ResolveJobBindings(design, tmpl)
	byJob := make(map[string]Binding, len(bindings))
	for _, b := range bindings {
		byJob[b.Job] = b
	}

	variationID := designVariationID(design)
	for _, job := range tmpl.Jobs {
		if !jobRequirementApplies(job, variationID) {
			continue
		}
		allowed := append([]string(nil), job.AllowedEngines...)
		b, bound := byJob[job.ID]
		if !bound {
			if job.Required {
				return &JobBindingError{
					Job:    job.ID,
					Err:    ErrJobUnbound,
					Reason: strings.Join(allowed, ", "),
				}
			}
			continue
		}
		if b.NodeType == "" {
			return &JobBindingError{
				Job:    job.ID,
				NodeID: b.NodeID,
				Err:    ErrBindingNoNode,
			}
		}
		if b.Engine == "" {
			if job.Required {
				return &JobBindingError{Job: job.ID, Err: ErrJobUnbound, Reason: strings.Join(allowed, ", ")}
			}
			continue
		}
		if !engineInList(b.Engine, allowed) {
			return &JobBindingError{
				Job:    job.ID,
				Engine: b.Engine,
				Err:    ErrEngineNotAllowed,
				Reason: strings.Join(allowed, ", "),
			}
		}
		pair, ok := cat.Pair(job.ID, b.Engine)
		if !ok {
			return &JobBindingError{
				Job:    job.ID,
				Engine: b.Engine,
				Err:    ErrEngineNotAllowed,
				Reason: strings.Join(allowed, ", "),
			}
		}
		if pair.Fit == engines.FitImpossible {
			return &JobBindingError{
				Job:    job.ID,
				Engine: b.Engine,
				Reason: pair.Reason,
				Err:    ErrImpossiblePair,
			}
		}
		if pair.Build != engines.BuildAvailable {
			return &JobBindingError{
				Job:    job.ID,
				Engine: b.Engine,
				Err:    ErrEngineNotAllowed,
				Reason: strings.Join(allowed, ", "),
			}
		}
	}
	return nil
}

func validateJobBindingsContract(design DeploymentDesign, tmpl *templates.TemplateSpec) error {
	return ValidateJobBindings(design, tmpl, engines.Default())
}

func designVariationID(design DeploymentDesign) string {
	arch := strings.TrimSpace(design.Archetype)
	if arch == "" || arch == "standard" {
		return ""
	}
	return arch
}

func jobRequirementApplies(job templates.JobRequirement, variationID string) bool {
	if len(job.Variations) == 0 {
		return true
	}
	if variationID == "" {
		return false
	}
	for _, v := range job.Variations {
		if v == variationID {
			return true
		}
	}
	return false
}

func engineInList(engine string, allowed []string) bool {
	engine = strings.ToLower(strings.TrimSpace(engine))
	for _, a := range allowed {
		if strings.ToLower(strings.TrimSpace(a)) == engine {
			return true
		}
	}
	return false
}

func jobUnboundMessage(jobID, allowedCSV string) string {
	allowed := strings.TrimSpace(allowedCSV)
	if jobID == "timeline" && allowed != "" {
		return fmt.Sprintf(
			"timeline job is not bound: add a cache node, or bind timeline to a node with an allowed engine (%s)",
			allowed,
		)
	}
	if allowed != "" {
		return fmt.Sprintf("%s job is not bound: allowed engines are %s", jobID, allowed)
	}
	return fmt.Sprintf("%s job is not bound", jobID)
}
