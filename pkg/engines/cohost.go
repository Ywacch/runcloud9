package engines

import (
	"fmt"
	"strings"
)

// PairCoHost reports whether pair allows sharing a node (default true).
func PairCoHost(p Pair) bool {
	if p.CoHost != nil {
		return *p.CoHost
	}
	return true
}

// CanCoHost decides whether jobs may share one node running engine.
func CanCoHost(c *Catalog, engine string, jobs []string) (bool, string) {
	if c == nil {
		return false, "catalog missing"
	}
	engine = strings.ToLower(strings.TrimSpace(engine))
	if engine == "" {
		return false, "unknown engine"
	}
	if _, ok := c.EngineByID(engine); !ok {
		return false, fmt.Sprintf("unknown engine %q", engine)
	}
	if len(jobs) == 0 {
		return false, "no jobs"
	}
	if len(jobs) == 1 {
		if _, ok := c.JobByID(jobs[0]); !ok {
			return false, fmt.Sprintf("unknown job %q", jobs[0])
		}
		return true, ""
	}
	engines := make([]string, len(jobs))
	for i := range jobs {
		engines[i] = engine
	}
	return canCoHostJobs(c, engines, jobs)
}

func canCoHostJobs(c *Catalog, engines, jobs []string) (bool, string) {
	if len(jobs) != len(engines) {
		return false, "jobs and engines length mismatch"
	}
	for i, job := range jobs {
		if _, ok := c.JobByID(job); !ok {
			return false, fmt.Sprintf("unknown job %q", job)
		}
		eng := strings.ToLower(strings.TrimSpace(engines[i]))
		if _, ok := c.EngineByID(eng); !ok {
			return false, fmt.Sprintf("unknown engine %q", eng)
		}
	}
	uniqueEngines := map[string]struct{}{}
	for _, eng := range engines {
		uniqueEngines[strings.ToLower(strings.TrimSpace(eng))] = struct{}{}
	}
	if len(uniqueEngines) > 1 {
		return false, "jobs are on different engines"
	}
	engine := strings.ToLower(strings.TrimSpace(engines[0]))
	for i, job := range jobs {
		p, ok := c.Pair(job, engine)
		if !ok {
			return false, fmt.Sprintf("unknown pair %s/%s", job, engine)
		}
		if !PairCoHost(p) {
			return false, fmt.Sprintf("pair %s/%s cannot co-host", job, engine)
		}
		_ = i
	}
	var hasEvicting, hasNonEvicting bool
	for _, job := range jobs {
		j, ok := c.JobByID(job)
		if !ok {
			continue
		}
		for _, need := range j.Needs {
			switch need {
			case NeedEvicting:
				hasEvicting = true
			case NeedNonEvicting:
				hasNonEvicting = true
			}
		}
	}
	if hasEvicting && hasNonEvicting {
		for _, job := range jobs {
			j, ok := c.JobByID(job)
			if !ok {
				continue
			}
			nonEvict := false
			for _, need := range j.Needs {
				if need == NeedNonEvicting {
					nonEvict = true
					break
				}
			}
			if !nonEvict {
				continue
			}
			p, ok := c.Pair(job, engine)
			if ok && p.TTLSeparatesEviction {
				return true, ""
			}
		}
		return false, "eviction policies conflict"
	}
	return true, ""
}
