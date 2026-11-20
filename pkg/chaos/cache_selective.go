package chaos

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/placement"
	"github.com/ywacch/cloud9/pkg/templates"
)

const selectiveUnlinkLua = `
for i = 1, #ARGV do
  local pattern = ARGV[i]
  local cursor = "0"
  repeat
    local r = redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", 200)
    cursor = r[1]
    for _, k in ipairs(r[2]) do
      redis.call("UNLINK", k)
    end
  until cursor == "0"
end
return 1
`

func cacheNodeSharesWorkQueue(design deployer.DeploymentDesign, targetComponent string) bool {
	if design.ResolvedPlacement == nil {
		return false
	}
	return nodeHostsJob(design.ResolvedPlacement, targetComponent, "work_queue")
}

func nodeHostsJob(topo *placement.Topology, targetComponent, jobID string) bool {
	if topo == nil {
		return false
	}
	node, ok := topo.NodeForComponent(targetComponent)
	if !ok {
		return false
	}
	for _, j := range node.Jobs {
		if j == jobID {
			return true
		}
	}
	return false
}

func cacheIntrospectionKeyPatterns(design deployer.DeploymentDesign) []string {
	tmpl := templates.GetTemplate(design.Template)
	if tmpl == nil || len(tmpl.Introspection) == 0 {
		return nil
	}
	spec, ok := tmpl.Introspection["cache"]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(spec.KeyPatterns))
	for _, p := range spec.KeyPatterns {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func redisCLISelectiveFlushArgv(patterns []string) ([]string, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("chaos: cache selective flush requires keyPatterns")
	}
	args := []string{"redis-cli", "EVAL", selectiveUnlinkLua, "0"}
	args = append(args, patterns...)
	return args, nil
}

// selectiveUnlinkKeyPatterns unlinks keys that match patterns. Tests use a redis client directly.
func selectiveUnlinkKeyPatterns(ctx context.Context, client *redis.Client, patterns []string) error {
	if client == nil {
		return fmt.Errorf("chaos: redis client is required")
	}
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, pattern, 200).Result()
			if err != nil {
				return fmt.Errorf("chaos: scan %q: %w", pattern, err)
			}
			if len(keys) > 0 {
				if err := client.Unlink(ctx, keys...).Err(); err != nil {
					return fmt.Errorf("chaos: unlink: %w", err)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	return nil
}
