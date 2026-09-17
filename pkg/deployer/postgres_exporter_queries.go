package deployer

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	return dbReadReplicaCount(cfg) >= 1
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

func postgresExporterExtendQueryArgs() []string {
	return []string{postgresExporterExtendQueryPathFlag}
}
