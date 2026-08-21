package deployer

import (
	"context"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	dbReplicaAppLabel       = "postgres-replica"
	dbReplicaDeploymentName = "postgres-replica"
	dbReplicaServiceName    = "db-replica"
	dbReplicaDataVolumeName = "pgdata"
	dbReplicaEphemeralSize  = "500Mi"
)

// postgresPrimaryCommand returns the postgres primary command, including sync flags when configured.
func postgresPrimaryCommand(nodeConfig map[string]any) []string {
	spec := componentRegistry["db"]
	cmd := append([]string(nil), spec.Command...)
	mode, err := parseReplicationMode(nodeConfig)
	if err != nil {
		mode = "async"
	}
	if mode == "sync" && parseReadReplicaCount(nodeConfig) >= 1 {
		cmd = append(cmd,
			"-c", "synchronous_commit=on",
			"-c", "synchronous_standby_names=*",
		)
	}
	return cmd
}

func dbReadReplicaCount(cfg DeployConfig) int32 {
	if !dbEngineSupportsStreamingReplicas(dbEngineFromConfig(cfg.Node.Config)) {
		return 0
	}
	return parseReadReplicaCount(cfg.Node.Config)
}

func dbReplicaResources(base corev1.ResourceRequirements) corev1.ResourceRequirements {
	out := *base.DeepCopy()
	qty := resource.MustParse(dbReplicaEphemeralSize)
	if out.Limits == nil {
		out.Limits = corev1.ResourceList{}
	}
	if out.Requests == nil {
		out.Requests = corev1.ResourceList{}
	}
	out.Limits[corev1.ResourceEphemeralStorage] = qty.DeepCopy()
	out.Requests[corev1.ResourceEphemeralStorage] = qty.DeepCopy()
	return out
}

func dbReplicaDeploymentSpec(cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) *appsv1.Deployment {
	dbSpec := componentRegistry["db"]
	replicaResources := dbReplicaResources(resources)
	labels := map[string]string{"app": dbReplicaAppLabel, "deployment": cfg.Namespace}

	tcpPort := intstr.FromInt32(dbSpec.Ports[0])
	readiness := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: tcpPort},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       3,
		FailureThreshold:    5,
	}

	podAnnotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   strconv.Itoa(int(dbSpec.Exporter.Port)),
		"prometheus.io/path":   dbSpec.Exporter.MetricsPath,
	}

	initContainer := corev1.Container{
		Name:    "basebackup",
		Image:   dbSpec.Image,
		Command: []string{"sh", "-c", "until pg_isready -h db; do sleep 1; done; exec pg_basebackup -h db -U syscrack -D $PGDATA -Fp -Xs -R -P"},
		Env: []corev1.EnvVar{
			{Name: "PGDATA", Value: "/var/lib/postgresql/data"},
		},
		Resources: replicaResources,
		VolumeMounts: []corev1.VolumeMount{{
			Name:      dbReplicaDataVolumeName,
			MountPath: "/var/lib/postgresql/data",
		}},
	}

	mainContainer := corev1.Container{
		Name:  dbReplicaAppLabel,
		Image: dbSpec.Image,
		Ports: []corev1.ContainerPort{{ContainerPort: dbSpec.Ports[0]}},
		Env: []corev1.EnvVar{
			{Name: "PGDATA", Value: "/var/lib/postgresql/data"},
		},
		Resources:      replicaResources,
		ReadinessProbe: readiness,
		VolumeMounts: []corev1.VolumeMount{{
			Name:      dbReplicaDataVolumeName,
			MountPath: "/var/lib/postgresql/data",
		}},
	}

	exporter := dbSpec.Exporter
	containers := []corev1.Container{
		mainContainer,
		{
			Name:      exporter.Name,
			Image:     exporter.Image,
			Ports:     []corev1.ContainerPort{{ContainerPort: exporter.Port}},
			Resources: ExporterResources(),
			Env:       envMapToK8s(exporter.Env),
		},
	}

	sizeLimit := resource.MustParse(dbReplicaEphemeralSize)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: dbReplicaDeploymentName, Namespace: cfg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: podAnnotations},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{initContainer},
					Containers:     containers,
					Volumes: []corev1.Volume{{
						Name: dbReplicaDataVolumeName,
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{
								SizeLimit: &sizeLimit,
							},
						},
					}},
				},
			},
		},
	}
}

func dbReplicaServiceSpec(cfg DeployConfig) *corev1.Service {
	dbSpec := componentRegistry["db"]
	labels := map[string]string{"app": dbReplicaAppLabel, "deployment": cfg.Namespace}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: dbReplicaServiceName, Namespace: cfg.Namespace},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "postgres",
				Port:       dbSpec.ServicePort,
				TargetPort: intstr.FromInt32(dbSpec.Ports[0]),
			}},
		},
	}
}

func (k *K8sDeployer) deployDBReadReplicas(ctx context.Context, cfg DeployConfig, resources corev1.ResourceRequirements, replicas int32) error {
	if replicas <= 0 {
		return nil
	}

	dep := dbReplicaDeploymentSpec(cfg, resources, replicas)
	if _, err := k.clientset.AppsV1().Deployments(cfg.Namespace).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create postgres-replica deployment: %w", err)
	}

	svc := dbReplicaServiceSpec(cfg)
	if _, err := k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create db-replica service: %w", err)
	}

	return k.waitForReady(ctx, cfg.Namespace, dbReplicaAppLabel, replicas)
}
