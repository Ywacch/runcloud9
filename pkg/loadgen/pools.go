package loadgen

import (
	"context"

	"github.com/ywacch/cloud9/pkg/templates"
)

// capIdentityPools returns pools limited to the first n entries of each slice.
// n <= 0 leaves pools unchanged.
func capIdentityPools(pools map[string][]string, n int) map[string][]string {
	if n <= 0 || len(pools) == 0 {
		return pools
	}
	out := make(map[string][]string, len(pools))
	for k, v := range pools {
		out[k] = capIdentitySlice(v, n)
	}
	return out
}

func capIdentitySlice(v []string, n int) []string {
	if n <= 0 || len(v) <= n {
		return v
	}
	return append([]string(nil), v[:n]...)
}

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
