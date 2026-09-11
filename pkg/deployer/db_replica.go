package deployer

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	dbReplicaAppLabel            = "postgres-replica"
	dbReplicaStatefulSetName     = "postgres-replica"
	dbReplicaHeadlessServiceName = "postgres-replica"
	DBReplicaServiceName         = "db-replica"
	dbReplicaDataVolumeName      = "pgdata"
	dbReplicaEphemeralSize       = "500Mi"
	// --checkpoint=fast: after pg_restore the primary is dirty; default spread
	// checkpoint left replica inits on "waiting for checkpoint" until the 5m deploy deadline.
	dbReplicaBasebackupScript = `until pg_isready -h db -U syscrack; do sleep 1; done; pg_basebackup -h db -U syscrack -D $PGDATA -Fp -Xs -R -P --checkpoint=fast; echo "primary_conninfo = 'host=db port=5432 user=syscrack application_name=${HOSTNAME} sslmode=disable'" >> "$PGDATA/postgresql.auto.conf"`
)

// postgresPrimaryCommand returns the postgres primary command, including sync flags when configured.
func postgresPrimaryCommand(nodeConfig map[string]any) []string {
	spec := componentRegistry["db"]
	cmd := append([]string(nil), spec.Command...)
	cmd = append(cmd, postgresSynchronousReplicationArgs(nodeConfig)...)
	return cmd
}

// postgresSynchronousReplicationArgs returns -c flag pairs for synchronous replication settings.
func postgresSynchronousReplicationArgs(nodeConfig map[string]any) []string {
	readReplicas := parseReadReplicaCount(nodeConfig)
	if readReplicas < 1 {
		return nil
	}
	mode, err := parseReplicationMode(nodeConfig)
	if err != nil || !requiresSynchronousReplication(mode) {
		return nil
	}
	level := parseSyncCommitLevel(nodeConfig)
	var standbyNames string
	switch mode {
	case replicationModeSemiSync:
		standbyNames = "ANY 1 (*)"
	case replicationModeSync:
		standbyNames = fmt.Sprintf("FIRST %d (*)", readReplicas)
	default:
		return nil
	}
	return []string{
		"-c", "synchronous_commit=" + level,
		"-c", "synchronous_standby_names=" + standbyNames,
	}
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

// dbResourcesForSize resolves postgres workload resources for one instance size.
func dbResourcesForSize(size string) corev1.ResourceRequirements {
	return ResolveNodeResources("db", map[string]any{
		"instanceSize":    normalizeInstanceSize(size),
		"resourceProfile": normalizeInstanceSize(size),
	})
}

// dbReplicaHostnames returns stable DNS hostnames for deployed read replicas.
// Uniform sizes use one StatefulSet (postgres-replica-0, postgres-replica-1, ...).
// Mixed size groups use one StatefulSet per group (postgres-replica-0, postgres-replica-small-0, ...).
func dbReplicaHostnames(readReplicas int, standbySizes []string) []string {
	if readReplicas <= 0 {
		return nil
	}
	groups := sizeGroups(standbySizes)
	if len(groups) == 1 {
		hosts := make([]string, 0, readReplicas)
		for i := 0; i < readReplicas; i++ {
			hosts = append(hosts, fmt.Sprintf("%s-%d", dbReplicaStatefulSetName, i))
		}
		return hosts
	}
	hosts := make([]string, 0, readReplicas)
	for i, group := range groups {
		stsName := dbReplicaStatefulSetName
		if i > 0 {
			stsName = dbReplicaStatefulSetName + "-" + group.Size
		}
		for ord := int32(0); ord < group.Count; ord++ {
			hosts = append(hosts, fmt.Sprintf("%s-%d", stsName, ord))
		}
	}
	return hosts
}

func dbReplicaStatefulSetNameForGroup(groupIndex int, size string) string {
	if groupIndex == 0 {
		return dbReplicaStatefulSetName
	}
	return dbReplicaStatefulSetName + "-" + size
}

func dbReplicaPodTemplateSpec(
	cfg DeployConfig,
	resources corev1.ResourceRequirements,
	size string,
) corev1.PodTemplateSpec {
	dbSpec := componentRegistry["db"]
	replicaResources := dbReplicaResources(resources)
	labels := map[string]string{
		"app":                dbReplicaAppLabel,
		"deployment":         cfg.Namespace,
		instanceSizeLabelKey: normalizeInstanceSize(size),
	}

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
		Name:  "basebackup",
		Image: dbSpec.Image,
		// Append primary_conninfo after pg_basebackup -R so application_name matches pod HOSTNAME
		// (used by primary-side replay_lag metrics and db instance layers).
		Command: []string{"sh", "-c", dbReplicaBasebackupScript},
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
	return corev1.PodTemplateSpec{
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
	}
}

func dbReplicaStatefulSetSpec(
	cfg DeployConfig,
	resources corev1.ResourceRequirements,
	replicas int32,
	size string,
) *appsv1.StatefulSet {
	return dbReplicaStatefulSetSpecNamed(cfg, dbReplicaStatefulSetName, resources, replicas, size)
}

func dbReplicaStatefulSetSpecNamed(
	cfg DeployConfig,
	stsName string,
	resources corev1.ResourceRequirements,
	replicas int32,
	size string,
) *appsv1.StatefulSet {
	labels := map[string]string{
		"app":                dbReplicaAppLabel,
		"deployment":         cfg.Namespace,
		instanceSizeLabelKey: normalizeInstanceSize(size),
	}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: cfg.Namespace},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         dbReplicaHeadlessServiceName,
			Replicas:            ptr(replicas),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: labels},
			Template:            dbReplicaPodTemplateSpec(cfg, resources, size),
		},
	}
}

func dbReplicaHeadlessServiceSpec(cfg DeployConfig) *corev1.Service {
	dbSpec := componentRegistry["db"]
	labels := map[string]string{"app": dbReplicaAppLabel, "deployment": cfg.Namespace}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: dbReplicaHeadlessServiceName, Namespace: cfg.Namespace},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labels,
			Ports: []corev1.ServicePort{{
				Name:       "postgres",
				Port:       dbSpec.ServicePort,
				TargetPort: intstr.FromInt32(dbSpec.Ports[0]),
			}},
		},
	}
}

func dbReplicaServiceSpec(cfg DeployConfig) *corev1.Service {
	dbSpec := componentRegistry["db"]
	labels := map[string]string{"app": dbReplicaAppLabel, "deployment": cfg.Namespace}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: DBReplicaServiceName, Namespace: cfg.Namespace},
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

func (k *K8sDeployer) createDBReadReplicaObjects(ctx context.Context, cfg DeployConfig, readReplicas int32) (int32, error) {
	if readReplicas <= 0 {
		return 0, nil
	}

	total := 1 + int(readReplicas)
	sizes := instanceSizesFromConfig(cfg.Node.Config, total)
	if len(sizes) < total {
		return 0, fmt.Errorf("deployDBReadReplicas: expected %d instance sizes, got %d", total, len(sizes))
	}
	standbySizes := sizes[1:]
	groups := sizeGroups(standbySizes)

	headless := dbReplicaHeadlessServiceSpec(cfg)
	if _, err := k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, headless, metav1.CreateOptions{}); err != nil {
		return 0, fmt.Errorf("create postgres-replica headless service: %w", err)
	}

	var desiredReady int32
	for i, group := range groups {
		stsName := dbReplicaStatefulSetNameForGroup(i, group.Size)
		resources := dbResourcesForSize(group.Size)
		sts := dbReplicaStatefulSetSpecNamed(cfg, stsName, resources, group.Count, group.Size)
		if _, err := k.clientset.AppsV1().StatefulSets(cfg.Namespace).Create(ctx, sts, metav1.CreateOptions{}); err != nil {
			return 0, fmt.Errorf("create postgres-replica statefulset %s: %w", stsName, err)
		}
		desiredReady += group.Count
	}

	svc := dbReplicaServiceSpec(cfg)
	if _, err := k.clientset.CoreV1().Services(cfg.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		return 0, fmt.Errorf("create db-replica service: %w", err)
	}
	return desiredReady, nil
}

func (k *K8sDeployer) deployDBReadReplicas(ctx context.Context, cfg DeployConfig, readReplicas int32) error {
	desiredReady, err := k.createDBReadReplicaObjects(ctx, cfg, readReplicas)
	if err != nil {
		return err
	}
	if desiredReady <= 0 {
		return nil
	}

	prefix := []PodWaitProgress{{Ordinal: 1, Ready: true, WaitCode: WaitCodeReady}}
	return k.waitForReady(ctx, cfg, dbReplicaAppLabel, desiredReady, waitProgressOpts{prefix: prefix})
}

// dbReplicaHostsFromConfig builds READ_REPLICA_HOSTS for a db node config.
func dbReplicaHostsFromConfig(dbConfig map[string]any) string {
	readReplicas := int(parseReadReplicaCount(dbConfig))
	if readReplicas <= 0 {
		return ""
	}
	total := 1 + readReplicas
	sizes := instanceSizesFromConfig(dbConfig, total)
	if len(sizes) < total {
		return ""
	}
	hosts := dbReplicaHostnames(readReplicas, sizes[1:])
	return strings.Join(hosts, ",")
}
