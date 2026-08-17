package loadgen

import (
	"context"

	"github.com/ywacch/cloud9/pkg/templates"
)

func fetchSegmentPools(ctx context.Context, baseURL string, params []templates.LoadgenActorParam) map[string][]string {
	out := make(map[string][]string)
	seen := make(map[string]struct{})
	for _, param := range params {
		for _, pool := range param.Pools {
			key := segmentPoolKey(pool.PoolEndpoint, pool.PoolField, pool.Name)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			values := fetchPool(ctx, baseURL, pool.PoolEndpoint, pool.PoolField)
			if len(values) > 0 {
				out[key] = values
			}
		}
	}
	return out
}

func actorSegmentPoolsFromTemplate(ctx context.Context, baseURL, templateID string) map[string][]string {
	params, ok := templates.LoadgenParams(templateID)
	if !ok {
		return nil
	}
	return fetchSegmentPools(ctx, baseURL, params)
}
