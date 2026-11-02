package deployer

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	dbReplicaAppLabel            = "postgres-replica"
	dbReplicaStatefulSetName     = "postgres-replica"
	dbReplicaHeadlessServiceName = "postgres-replica"
	DBReplicaServiceName         = "db-replica"
)

// DBReplicaAppLabel returns the K8s app label for postgres read-replica pods.
func DBReplicaAppLabel() string { return dbReplicaAppLabel }

// DBReplicaHeadlessServiceName returns the headless Service name for replica pod DNS.
func DBReplicaHeadlessServiceName() string { return dbReplicaHeadlessServiceName }

const (
	dbReplicaDataVolumeName = "pgdata"
	dbReplicaEphemeralSize  = "500Mi"
	// --checkpoint=fast: after pg_restore the primary is dirty; default spread
	// checkpoint left replica inits on "waiting for checkpoint" until the 5m deploy deadline.
	dbReplicaBasebackupScriptCore = `until pg_isready -h db -U syscrack; do sleep 1; done; SLOT=$(printf '%s' "$HOSTNAME" | tr '-' '_'); psql -h db -U syscrack -d syscrack -v ON_ERROR_STOP=1 -tAc "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '${SLOT}' AND NOT active"; psql -h db -U syscrack -d syscrack -v ON_ERROR_STOP=1 -tAc "SELECT pg_create_physical_replication_slot('${SLOT}') WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '${SLOT}')"; pg_basebackup -h db -U syscrack -D $PGDATA -Fp -Xs -R -P --checkpoint=fast; echo "primary_conninfo = 'host=db port=5432 user=syscrack application_name=${HOSTNAME} sslmode=disable'" >> "$PGDATA/postgresql.auto.conf"; echo "primary_slot_name = '${SLOT}'" >> "$PGDATA/postgresql.auto.conf"`
)

// DBReplicaHostnamesFromConfig returns standby pod hostnames in deploy order.
func DBReplicaHostnamesFromConfig(config map[string]any) []string {
	readReplicas := int(parseReadReplicaCount(config))
	if readReplicas <= 0 {
		return nil
	}
	total := 1 + readReplicas
	sizes := instanceSizesFromConfig(config, total)
	return dbReplicaHostnames(readReplicas, sizes[1:])
}

func buildDBReplicaInitScript(cfg DeployConfig) string {
	readReplicas := int(dbReadReplicaCount(cfg))
	if readReplicas <= 0 {
		return dbReplicaBasebackupScriptCore
	}
	hosts := DBReplicaHostnamesFromConfig(cfg.Node.Config)
	delays, err := ParseReplicaApplyDelayMs(cfg.Node.Config, readReplicas)
	if err != nil || len(hosts) != len(delays) {
		delays = zeroDelays(readReplicas)
	}
	delayer, err := LookupReplicaDelayer(dbEngineFromConfig(cfg.Node.Config))
	if err != nil {
		return dbReplicaBasebackupScriptCore
	}
	return dbReplicaBasebackupScriptCore + delayer.InitScript(hosts, delays)
}

func replicaApplyDelayCaseScript(hosts []string, delaysMs []int) string {
	if len(hosts) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("; case \"$HOSTNAME\" in")
	for i, host := range hosts {
		ms := 0
		if i < len(delaysMs) {
			ms = delaysMs[i]
		}
		b.WriteString("\n  ")
		b.WriteString(host)
		b.WriteString(")\n    echo \"recovery_min_apply_delay = '")
		b.WriteString(strconv.Itoa(ms))
		b.WriteString("ms'\" >> \"$PGDATA/postgresql.auto.conf\"\n    ;;")
	}
	b.WriteString("\nesac")
	return b.String()
}

// postgresPrimaryCommand returns the postgres primary container command.
// Synchronous replication GUCs are applied after standbys stream (see postgres_sync.go).
func postgresPrimaryCommand(_ map[string]any) []string {
	spec := componentRegistry["db"]
	return append([]string(nil), spec.Command...)
}

func dbReadReplicaCount(cfg DeployConfig) int32 {
	if !dbEngineSupportsStreamingReplicas(dbEngineFromConfig(cfg.Node.Config)) {
		return 0
	}
	return parseReadReplicaCount(cfg.Node.Config)
}

func dbReplicaResources(base corev1.ResourceRequirements) corev1.ResourceRequirements {
	return dbPostgresEphemeralResources(base)
}

// dbPrimaryResources raises ephemeral-storage on the postgres primary Deployment.
// PGDATA lives on the container filesystem; wal_keep_size needs headroom beyond the catalog default.
func dbPrimaryResources(base corev1.ResourceRequirements) corev1.ResourceRequirements {
	return dbPostgresEphemeralResources(base)
}

func dbPostgresEphemeralResources(base corev1.ResourceRequirements) corev1.ResourceRequirements {
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
		// After pg_basebackup -R, overwrite primary_conninfo so application_name is the pod
		// hostname (primary-side replay_lag labels). primary_slot_name is a GUC, not a libpq
		// option; stuffing it into primary_conninfo makes walreceiver fail with
		// invalid connection option "primary_slot_name".
		Command: []string{"sh", "-c", buildDBReplicaInitScript(cfg)},
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
	exporterContainer := corev1.Container{
		Name:      exporter.Name,
		Image:     exporter.Image,
		Ports:     []corev1.ContainerPort{{ContainerPort: exporter.Port}},
		Resources: ExporterResources(),
		Env:       envMapToK8s(exporter.Env),
	}

	sizeLimit := resource.MustParse(dbReplicaEphemeralSize)
	volumes := []corev1.Volume{{
		Name: dbReplicaDataVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				SizeLimit: &sizeLimit,
			},
		},
	}}
	if postgresExporterQueriesEnabled(cfg) {
		exporterContainer.Args = postgresExporterExtendQueryArgs()
		exporterContainer.VolumeMounts = append(exporterContainer.VolumeMounts, postgresExporterReplicaQueriesVolumeMount())
		volumes = append(volumes, postgresExporterQueriesVolume())
	}

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: podAnnotations},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{initContainer},
			Containers:     []corev1.Container{mainContainer, exporterContainer},
			Volumes:        volumes,
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

	if postgresExporterQueriesEnabled(cfg) {
		queriesCM := postgresExporterQueriesConfigMap(cfg.Namespace)
		if _, err := k.clientset.CoreV1().ConfigMaps(cfg.Namespace).Create(ctx, queriesCM, metav1.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return 0, fmt.Errorf("create postgres-exporter-queries configmap: %w", err)
			}
		}
	}

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
