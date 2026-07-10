package api

import (
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
)

// MetricsTopologyFromDeploymentDesign maps a stored deployment design to metrics.TopologyFlags.
// Callers (WebSocket stream, teardown) should use this so pkg/metrics stays free of pkg/deployer.
func MetricsTopologyFromDeploymentDesign(d *deployer.DeploymentDesign, template string) metrics.TopologyFlags {
	st := deployer.StorytellingTopologyFromDesign(d, template)
	return metrics.TopologyFlags{
		HasLB:         st.HasLB,
		HasMQ:         st.HasMQ,
		HasWorker:     st.HasWorker,
		HasCache:      st.HasCache,
		CacheStrategy: st.CacheStrategy,
		Template:      st.Template,
		HasDB:         st.HasDB,
		DBEngine:      st.DBEngine,
		DBRole:        st.DBRole,
		CacheEngine:   st.CacheEngine,
		CacheRole:     st.CacheRole,
		HasProxyCache: st.HasProxyCache,
	}
}
