package api

import (
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
)

// MetricsTopologyFromDeploymentDesign maps a stored deployment design to metrics.TopologyFlags.
// Callers (WebSocket stream, teardown) should use this so pkg/metrics stays free of pkg/deployer.
func MetricsTopologyFromDeploymentDesign(d *deployer.DeploymentDesign, template string) metrics.TopologyFlags {
	st := deployer.StorytellingTopologyFromDesign(d, template)
	topo := metrics.TopologyFlags{
		HasLB:            st.HasLB,
		HasMQ:            st.HasMQ,
		HasWorker:        st.HasWorker,
		HasCache:         st.HasCache,
		CacheStrategy:    st.CacheStrategy,
		Template:         st.Template,
		HasDB:            st.HasDB,
		DBEngine:         st.DBEngine,
		DBRole:           st.DBRole,
		CacheEngine:      st.CacheEngine,
		CacheRole:        st.CacheRole,
		HasProxyCache:    st.HasProxyCache,
		DBReadReplicas:   st.ReadReplicaCount,
		ReplicationMode:  st.ReplicationMode,
		SyncCommitLevel:  st.SyncCommitLevel,
		SyncStandbyNames: st.SyncStandbyNames,
	}
	if d != nil {
		for _, n := range d.Nodes {
			if n.Type != "db" {
				continue
			}
			rr := int(deployer.ParseReadReplicaCount(n.Config))
			topo.DBReadConsistency = deployer.ParseReadConsistency(n.Config)
			if delays, err := deployer.ParseReplicaApplyDelayMs(n.Config, rr); err == nil && len(delays) > 0 {
				topo.DBReplicaApplyDelayMs = delays
			}
		}
	}
	return topo
}
