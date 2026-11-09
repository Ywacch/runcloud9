package deployer

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ywacch/cloud9/pkg/templates"
)

const (
	postgresExporterQueriesConfigMapName   = "postgres-exporter-queries"
	postgresExporterQueriesVolumeName      = "postgres-exporter-queries"
	postgresExporterQueriesFileName        = "queries.yaml"
	postgresExporterReplicaQueriesFileName = "replica-queries.yaml"
	postgresExporterQueriesMountPath       = "/etc/postgres-exporter/" + postgresExporterQueriesFileName
	postgresExporterExtendQueryPathFlag    = "--extend.query-path=" + postgresExporterQueriesMountPath
)

// postgresExporterPrimaryQueriesYAML is mounted on the primary postgres-exporter sidecar.
// replication_replay_lag names are frozen (F8 / S7) — do not rename those metrics.
// wal_lsn is a numeric LSN-from-0/0 gauge for partition lag when a standby drops off pg_stat_replication.
// postgres-exporter v0.16.0 names custom metrics {block}_{column} without a pg_ prefix.
const postgresExporterPrimaryQueriesYAML = `replication_replay_lag:
  master: true
  query: |
    SELECT application_name, sync_state,
           COALESCE(EXTRACT(EPOCH FROM replay_lag), 0)::double precision AS replay_lag_seconds,
           COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), 0)::double precision AS replay_lag_bytes
    FROM pg_stat_replication
  metrics:
    - application_name:
        usage: LABEL
    - sync_state:
        usage: LABEL
    - replay_lag_seconds:
        usage: GAUGE
        description: "Primary-side replay lag in seconds per connected standby"
    - replay_lag_bytes:
        usage: GAUGE
        description: "Primary-side WAL bytes behind per connected standby"
wal_lsn:
  master: true
  query: |
    SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::double precision AS bytes
  metrics:
    - bytes:
        usage: GAUGE
        description: "Primary WAL insert LSN as bytes from 0/0"
c9_timeline:
  master: true
  query: |
    SELECT
      pg_total_relation_size(c.oid)::float8 AS relation_bytes,
      s.n_dead_tup::float8 AS dead_tuples
    FROM pg_class c
    JOIN pg_stat_user_tables s ON s.relid = c.oid
    WHERE c.relname = 'timelines'
      AND c.relkind = 'r'
  metrics:
    - relation_bytes:
        usage: GAUGE
        description: "Byte size of the timelines fan-out table"
    - dead_tuples:
        usage: GAUGE
        description: "Dead row estimate for timelines (vacuum pressure)"
`

// postgresExporterReplicaQueriesYAML is mounted on replica postgres-exporter sidecars only.
// Must not run pg_current_wal_lsn() on standbys (recovery is in progress).
const postgresExporterReplicaQueriesYAML = `wal_replay_lsn:
  master: false
  query: |
    SELECT pg_wal_lsn_diff(COALESCE(pg_last_wal_replay_lsn(), '0/0'), '0/0')::double precision AS bytes
  metrics:
    - bytes:
        usage: GAUGE
        description: "Standby replay LSN as bytes from 0/0"
`

func postgresExporterQueriesEnabled(cfg DeployConfig) bool {
	if cfg.Node.Type != "db" {
		return false
	}
	if dbReadReplicaCount(cfg) >= 1 {
		return true
	}
	return timelineJobOnNode(cfg, cfg.Node.ID, "postgresql")
}

func timelineJobOnNode(cfg DeployConfig, nodeID, engine string) bool {
	tmpl := templates.GetTemplate(cfg.Design.Template)
	for _, b := range ResolveJobBindings(cfg.Design, tmpl) {
		if b.Job == "timeline" && b.NodeID == nodeID && b.Engine == engine {
			return true
		}
	}
	return false
}

func postgresExporterQueriesConfigMap(namespace string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      postgresExporterQueriesConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			postgresExporterQueriesFileName:        postgresExporterPrimaryQueriesYAML,
			postgresExporterReplicaQueriesFileName: postgresExporterReplicaQueriesYAML,
		},
	}
}

func postgresExporterQueriesVolume() corev1.Volume {
	return corev1.Volume{
		Name: postgresExporterQueriesVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: postgresExporterQueriesConfigMapName,
				},
			},
		},
	}
}

func postgresExporterQueriesVolumeMount() corev1.VolumeMount {
	return postgresExporterQueriesVolumeMountFor(postgresExporterQueriesFileName)
}

func postgresExporterReplicaQueriesVolumeMount() corev1.VolumeMount {
	return postgresExporterQueriesVolumeMountFor(postgresExporterReplicaQueriesFileName)
}

func postgresExporterQueriesVolumeMountFor(subPath string) corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      postgresExporterQueriesVolumeName,
		MountPath: postgresExporterQueriesMountPath,
		SubPath:   subPath,
		ReadOnly:  true,
	}
}

// postgresExporterCollectorArgs disables built-in collectors that no signal reads.
// Custom queries in queries.yaml still run.
func postgresExporterCollectorArgs() []string {
	return []string{
		"--no-collector.locks",
		"--no-collector.replication",
		"--no-collector.replication_slot",
		"--no-collector.stat_bgwriter",
		"--no-collector.stat_wal_receiver",
		"--no-collector.statio",
		"--no-collector.wal",
	}
}

func postgresExporterExtendQueryArgs() []string {
	args := []string{postgresExporterExtendQueryPathFlag}
	return append(args, postgresExporterCollectorArgs()...)
}
