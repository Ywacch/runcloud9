package metrics

import (
	"context"
	"fmt"
	"strconv"
)

// topologyRequiresPostgresSyncGUCs reports whether the design expects synchronous replication GUCs.
func topologyRequiresPostgresSyncGUCs(topo TopologyFlags) bool {
	if !topo.HasDB || topo.DBReadReplicas < 1 || topo.SyncStandbyNames == "" {
		return false
	}
	if topo.SyncCommitLevel != "remote_apply" {
		return false
	}
	switch topo.ReplicationMode {
	case "semi-sync", "sync":
		return true
	default:
		return false
	}
}

func promQLStringLiteral(s string) string {
	return strconv.Quote(s)
}

// postgresSyncGUCsActive checks live pg_settings from postgres_exporter against the design.
func (c *MetricsCollector) postgresSyncGUCsActive(ctx context.Context, ns string, topo TopologyFlags) bool {
	ctx = withPromCaller(ctx, "db")
	if c == nil || !topologyRequiresPostgresSyncGUCs(topo) {
		return false
	}
	standbyCount, standbyPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`count(pg_settings{namespace=%q,name="synchronous_standby_names",setting=%s})`,
		ns, promQLStringLiteral(topo.SyncStandbyNames),
	))
	commitCount, commitPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`count(pg_settings{namespace=%q,name="synchronous_commit",setting=%s})`,
		ns, promQLStringLiteral(topo.SyncCommitLevel),
	))
	return standbyPresent && commitPresent && standbyCount > 0 && commitCount > 0
}
