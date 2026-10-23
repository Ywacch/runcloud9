package loadgen

import (
	"context"

	"github.com/ywacch/cloud9/pkg/templates"
)

func fetchSegmentPools(
	ctx context.Context,
	baseURL string,
	params []templates.LoadgenActorParam,
	idParam string,
	poolLimit int,
	specs []EndpointSpec,
) map[string][]string {
	epKeys := identityEndpointKeys(specs, idParam)
	out := make(map[string][]string)
	seen := make(map[string]struct{})
	for _, param := range params {
		limitPool := poolLimit > 0 && idParam != "" && segmentParamFeedsIdentity(param, epKeys)
		for _, pool := range param.Pools {
			key := segmentPoolKey(pool.PoolEndpoint, pool.PoolField, pool.Name)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			endpoint := pool.PoolEndpoint
			if limitPool {
				endpoint = withPoolLimit(endpoint, poolLimit)
			}
			values := fetchPool(ctx, baseURL, endpoint, pool.PoolField)
			if len(values) > 0 {
				out[key] = values
			}
		}
	}
	return out
}

func segmentParamFeedsIdentity(param templates.LoadgenActorParam, epKeys map[string]bool) bool {
	for _, k := range param.ApplyTo {
		if epKeys[k] {
			return true
		}
	}
	return false
}

func actorSegmentPoolsFromTemplate(
	ctx context.Context,
	baseURL, templateID string,
	idParam string,
	poolLimit int,
	specs []EndpointSpec,
) map[string][]string {
	params, ok := templates.LoadgenParams(templateID)
	if !ok {
		return nil
	}
	return fetchSegmentPools(ctx, baseURL, params, idParam, poolLimit, specs)
}
