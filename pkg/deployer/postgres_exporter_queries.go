package deployer

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	postgresExporterQueriesConfigMapName = "postgres-exporter-queries"
	postgresExporterQueriesVolumeName    = "postgres-exporter-queries"
	postgresExporterQueriesFileName      = "queries.yaml"
	postgresExporterQueriesMountPath     = "/etc/postgres-exporter/" + postgresExporterQueriesFileName
	postgresExporterExtendQueryPathFlag  = "--extend.query-path=" + postgresExporterQueriesMountPath
)

// postgresExporterQueriesYAML exposes primary-side pg_stat_replication.replay_lag in seconds.
// Query block name replication_replay_lag → metric pg_replication_replay_lag_replay_lag_seconds.
const postgresExporterQueriesYAML = `replication_replay_lag:
  master: true
  query: |
    SELECT application_name,
           COALESCE(EXTRACT(EPOCH FROM replay_lag), 0)::double precision AS replay_lag_seconds
    FROM pg_stat_replication
  metrics:
    - application_name:
        usage: LABEL
    - replay_lag_seconds:
        usage: GAUGE
        description: "Primary-side replay lag in seconds per connected standby"
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
			postgresExporterQueriesFileName: postgresExporterQueriesYAML,
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
	return corev1.VolumeMount{
		Name:      postgresExporterQueriesVolumeName,
		MountPath: postgresExporterQueriesMountPath,
		SubPath:   postgresExporterQueriesFileName,
		ReadOnly:  true,
	}
}

func postgresExporterExtendQueryArgs() []string {
	return []string{postgresExporterExtendQueryPathFlag}
}
