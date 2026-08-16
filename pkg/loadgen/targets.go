package loadgen

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// ResolvedTarget is a fully-built HTTP target for vegeta or the loadgen pod.
type ResolvedTarget struct {
	URL    string
	Method string
	Body   string
	Weight int
	// Key is the DistributionKey endpoint stratum (e.g. "GET /feed/{userId}").
	Key string
}

// profileToTargets resolves template endpoints, fetches param pools, and expands targets.
// profile must already be normalized (call NormalizeProfile first).
func profileToTargets(ctx context.Context, namespace string, profile LoadProfile) ([]ResolvedTarget, map[string]int, error) {
	if profile.Template == "" {
		return nil, nil, fmt.Errorf("loadgen: template is required")
	}
	specs, ok := cloneEndpointSpecs(profile.Template)
	if !ok {
		return nil, nil, fmt.Errorf("loadgen: unknown template %q", profile.Template)
	}
	if len(profile.Distribution) == 0 {
		return nil, nil, fmt.Errorf("loadgen: no distribution for template %q", profile.Template)
	}
	for i := range specs {
		if w, ok := profile.Distribution[DistributionKey(specs[i])]; ok {
			specs[i].Weight = w
		}
	}

	baseURL := resolveLoadgenBaseURL(namespace, profile)
	pools := fetchParamPools(ctx, baseURL, specs)
	distribution := EffectiveDistribution(specs)

	var targets []ResolvedTarget
	for _, spec := range specs {
		if spec.Weight <= 0 {
			continue
		}
		if spec.ParamName == "" {
			targets = append(targets, ResolvedTarget{
				URL:    baseURL + spec.PathTemplate,
				Method: spec.Method,
				Body:   spec.BodyTemplate,
				Weight: spec.Weight,
				Key:    DistributionKey(spec),
			})
			continue
		}

		pool := pools[spec.ParamName]
		if len(pool) == 0 {
			pool = []string{fallbackParamValue(spec.ParamName)}
		}
		weightEach := spec.Weight / len(pool)
		if weightEach <= 0 {
			weightEach = 1
		}
		placeholder := "{" + spec.ParamName + "}"
		for _, val := range pool {
			path := strings.Replace(spec.PathTemplate, placeholder, val, 1)
			targets = append(targets, ResolvedTarget{
				URL:    baseURL + path,
				Method: spec.Method,
				Body:   expandBodyTemplate(spec.BodyTemplate, val),
				Weight: weightEach,
				Key:    DistributionKey(spec),
			})
		}
	}

	if len(targets) == 0 {
		return nil, nil, fmt.Errorf("loadgen: no targets for template %q", profile.Template)
	}
	return targets, distribution, nil
}

func expandBodyTemplate(tmpl, param string) string {
	if tmpl == "" {
		return ""
	}
	out := strings.ReplaceAll(tmpl, "{{.Param}}", param)
	return out
}

// buildTargeterFromResolved creates a vegeta targeter from pre-resolved targets.
func buildTargeterFromResolved(targets []ResolvedTarget) vegeta.Targeter {
	var list []resolvedWeighted
	total := 0
	for _, t := range targets {
		w := t.Weight
		if w <= 0 {
			continue
		}
		list = append(list, resolvedWeighted{t: t, weight: w})
		total += w
	}
	if len(list) == 0 && len(targets) > 0 {
		list = append(list, resolvedWeighted{t: targets[0], weight: 1})
		total = 1
	}
	if total == 0 {
		total = 1
	}

	return newWeightedTargeter(list, total)
}

// BuildTargeterFromResolved exposes the resolved-target vegeta targeter for the loadgen pod.
func BuildTargeterFromResolved(targets []ResolvedTarget) vegeta.Targeter {
	return buildTargeterFromResolved(targets)
}

type resolvedWeighted struct {
	t      ResolvedTarget
	weight int
}

func newWeightedTargeter(list []resolvedWeighted, total int) vegeta.Targeter {
	var mu sync.Mutex
	return func(tgt *vegeta.Target) error {
		mu.Lock()
		defer mu.Unlock()

		r := rand.Intn(total)
		cumulative := 0
		var selected ResolvedTarget
		for _, w := range list {
			cumulative += w.weight
			if r < cumulative {
				selected = w.t
				break
			}
		}

		tgt.Method = selected.Method
		tgt.URL = selected.URL
		rid := fmt.Sprintf("%x", rand.Uint64())
		h := http.Header{}
		endpointKey := selected.Key
		if endpointKey == "" {
			endpointKey = selected.Method + " " + selected.URL
		}
		ApplyTraceSampleHeaders(h, rid, endpointKey, nil, TraceSampleRate())
		if selected.Body != "" {
			body := strings.ReplaceAll(selected.Body, "{{.Rand}}", strconv.Itoa(rand.Intn(1_000_000)))
			if strings.Contains(selected.Body, "{{.Param}}") {
				// Param should already be expanded; leave literal if present.
				body = strings.ReplaceAll(body, "{{.Param}}", "")
			}
			tgt.Body = []byte(body)
			h.Set("Content-Type", "application/json")
		} else {
			tgt.Body = nil
		}
		tgt.Header = h
		return nil
	}
}
