package metrics

import (
	"strings"

	"github.com/ywacch/cloud9/pkg/placement"
)

// collectScopedProm holds per-Collect label selectors for role-scoped PromQL.
type collectScopedProm struct {
	cacheRedis string
	memcached  string
}

func (c *MetricsCollector) beginCollectPromScope(ns string, pl *placement.Topology) {
	if c == nil {
		return
	}
	c.collectProm = collectScopedProm{}
	if pl == nil {
		return
	}
	cacheApp := jobComponentAppLabel(pl, "cache")
	if cacheApp != "" {
		c.collectProm.cacheRedis = promAppSelector(ns, cacheApp)
	}
	if cacheJobEngine(pl) == "memcached" && cacheApp != "" {
		c.collectProm.memcached = promAppSelector(ns, cacheApp)
	}
}

func (c *MetricsCollector) endCollectPromScope() {
	if c == nil {
		return
	}
	c.collectProm = collectScopedProm{}
}

func (c *MetricsCollector) cacheRedisPromSel(ns string) string {
	if c != nil && c.collectProm.cacheRedis != "" {
		return c.collectProm.cacheRedis
	}
	return promNamespaceSelector(ns)
}

func (c *MetricsCollector) memcachedPromSel(ns string) string {
	if c != nil && c.collectProm.memcached != "" {
		return c.collectProm.memcached
	}
	return promNamespaceSelector(ns)
}

func cacheJobEngine(pl *placement.Topology) string {
	if pl == nil {
		return ""
	}
	for _, j := range pl.Jobs {
		if j.Component == "cache" {
			return j.Engine
		}
	}
	return ""
}

func jobEngine(pl *placement.Topology, jobID string) string {
	if pl == nil {
		return ""
	}
	j, ok := pl.JobByID(jobID)
	if !ok {
		return ""
	}
	return j.Engine
}

func timelineOnPostgres(pl *placement.Topology) bool {
	return strings.EqualFold(jobEngine(pl, "timeline"), "postgresql")
}
