package metrics

// RunInfo labels a deployment row in a comparison pair (baseline or candidate).
type RunInfo struct {
	Role         string
	ComparisonID string
}

// SetRunInfo stores comparison metadata for metrics frames on this namespace.
func (c *MetricsCollector) SetRunInfo(namespace string, info RunInfo) {
	if c == nil || namespace == "" {
		return
	}
	c.runInfoMu.Lock()
	if c.runInfo == nil {
		c.runInfo = make(map[string]RunInfo)
	}
	if info.Role == "" && info.ComparisonID == "" {
		delete(c.runInfo, namespace)
	} else {
		c.runInfo[namespace] = info
	}
	c.runInfoMu.Unlock()
}

func (c *MetricsCollector) copyRunInfoOnto(dm *DeploymentMetrics) {
	if c == nil || dm == nil || dm.Namespace == "" {
		return
	}
	c.runInfoMu.RLock()
	info, ok := c.runInfo[dm.Namespace]
	c.runInfoMu.RUnlock()
	if !ok || info.Role == "" {
		return
	}
	dm.RunRole = info.Role
	dm.ComparisonID = info.ComparisonID
}

func (c *MetricsCollector) clearRunInfo(namespace string) {
	if c == nil || namespace == "" {
		return
	}
	c.runInfoMu.Lock()
	if c.runInfo != nil {
		delete(c.runInfo, namespace)
	}
	c.runInfoMu.Unlock()
}
