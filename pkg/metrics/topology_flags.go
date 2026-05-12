package metrics

// TopologyFlags summarizes the deployed topology for narrative generation.
// Kept intentionally simple — no coupling to deployer or template packages.
type TopologyFlags struct {
	HasCache      bool
	HasMQ         bool
	HasWorker     bool
	HasLB         bool
	CacheStrategy string
	Template      string
}

// TopologyFlagsFromDeployment derives coarse presence hints from the latest metrics snapshot.
// Components that are absent from the map or idle may read as absent; that only softens suggestions.
func TopologyFlagsFromDeployment(dm *DeploymentMetrics, template string) TopologyFlags {
	if dm == nil {
		return TopologyFlags{Template: template}
	}
	cache := dm.Components["cache"]
	mq := dm.Components["mq"]
	worker := dm.Components["worker"]
	lb := dm.Components["lb"]
	return TopologyFlags{
		HasCache:      cache.CPUUsage > 0 || cache.RedisOpsPerSec > 0,
		HasMQ:         mq.CPUUsage > 0 || mq.QueueDepth > 0,
		HasWorker:     worker.CPUUsage > 0,
		HasLB:         lb.CPUUsage > 0 || lb.ActiveConnections > 0,
		CacheStrategy: cache.CacheStrategy,
		Template:      template,
	}
}
