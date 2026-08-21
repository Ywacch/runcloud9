package deployer

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	postgresReplicationHBAConfigMapName = "postgres-replication-hba"
	postgresReplicationHBAVolumeName    = "postgres-replication-hba"
	postgresReplicationHBAScriptName    = "01-replication-hba.sh"
	postgresReplicationHBAMountPath     = "/docker-entrypoint-initdb.d/" + postgresReplicationHBAScriptName
)

// postgresReplicationHBAScript appends replication lines that POSTGRES_HOST_AUTH_METHOD=trust
// does not write. Official docker-entrypoint.sh only adds `host all all all trust`;
// pg_basebackup uses the replication protocol and needs `host replication ...`.
// Sourced (not executed) by the postgres image when the ConfigMap file is not +x.
// POSTGRES_USER is set by the official image (DefaultEnv POSTGRES_USER).
const postgresReplicationHBAScript = `echo "host replication ${POSTGRES_USER} 0.0.0.0/0 trust" >> "$PGDATA/pg_hba.conf"
echo "host replication ${POSTGRES_USER} ::/0 trust" >> "$PGDATA/pg_hba.conf"
`

func postgresReplicationHBAEnabled(cfg DeployConfig) bool {
	if cfg.Node.Type != "db" {
		return false
	}
	return dbEngineSupportsStreamingReplicas(dbEngineFromConfig(cfg.Node.Config))
}

func postgresReplicationHBAConfigMap(namespace string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      postgresReplicationHBAConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			postgresReplicationHBAScriptName: postgresReplicationHBAScript,
		},
	}
}

func postgresReplicationHBAVolume() corev1.Volume {
	return corev1.Volume{
		Name: postgresReplicationHBAVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: postgresReplicationHBAConfigMapName,
				},
			},
		},
	}
}

func postgresReplicationHBAVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      postgresReplicationHBAVolumeName,
		MountPath: postgresReplicationHBAMountPath,
		SubPath:   postgresReplicationHBAScriptName,
		ReadOnly:  true,
	}
}
