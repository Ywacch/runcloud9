package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

func float64Ptr(v float64) *float64 {
	return &v
}

func replicaLagSeconds(layer ComponentLayerMetrics) float64 {
	if layer.ReplicationLagSeconds == nil {
		return -1
	}
	return *layer.ReplicationLagSeconds
}

func TestPostgresPrimaryReplayLagByAppQuery(t *testing.T) {
	t.Parallel()
	q := postgresPrimaryReplayLagByAppQuery("ns-replica")
	for _, want := range []string{
		`namespace="ns-replica"`,
		`pod=~"postgres.*"`,
		`pod!~"postgres-replica.*"`,
		postgresExporterReplayLagMetric,
		"application_name",
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in query: %s", want, q)
		}
	}
}

func TestPostgresReplicaLagFallbackByPodQuery(t *testing.T) {
	t.Parallel()
	q := postgresReplicaLagFallbackByPodQuery("ns-fallback")
	if !strings.Contains(q, `namespace="ns-fallback"`) {
		t.Fatalf("missing namespace: %s", q)
	}
	if !strings.Contains(q, `pod=~"postgres-replica.*"`) {
		t.Fatalf("missing replica pod selector: %s", q)
	}
	if !strings.Contains(q, postgresExporterReplicaLagFallbackMetric) {
		t.Fatalf("missing fallback metric: %s", q)
	}
}

func TestPostgresXactRateByPodQuery(t *testing.T) {
	t.Parallel()
	q := postgresXactRateByPodQuery("ns-xact")
	for _, want := range []string{
		`namespace="ns-xact"`,
		`sum by (pod)`,
		`pg_stat_database_xact_commit`,
		`pg_stat_database_xact_rollback`,
		`datname!=""`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in query: %s", want, q)
		}
	}
}

func TestMapPostgresLayerRequestRates(t *testing.T) {
	t.Parallel()
	got := mapPostgresLayerRequestRates(map[string]float64{
		"postgres-abc":           12,
		"postgres-replica-roll":  3,
		"postgres-replica-other": 1,
	})
	if got[dbLayerIDPostgresPrimary] != 12 {
		t.Fatalf("primary rate: got %v, want 12", got[dbLayerIDPostgresPrimary])
	}
	if got["postgres-replica-roll"] != 3 || got["postgres-replica-other"] != 1 {
		t.Fatalf("replica rates: %+v", got)
	}
}

func TestCollectDBReplicaLayers_primaryResourceQueriesExcludeReplicas(t *testing.T) {
	t.Parallel()
	ns := "cloud9-primary-only-ns"
	replicaPod := "postgres-replica-only"
	desired := int32(1)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaDeploymentName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaPod,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(deployment, pod)

	var primaryCPUQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "container_cpu_usage_seconds_total") && strings.Contains(q, `pod=~"postgres.*"`):
			primaryCPUQuery = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.25"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	_ = c.collectDBReplicaLayers(context.Background(), ns)
	if primaryCPUQuery == "" {
		t.Fatal("expected primary CPU PromQL query")
	}
	if !strings.Contains(primaryCPUQuery, `pod!~"postgres-replica.*"`) {
		t.Fatalf("primary CPU query should exclude replica pods: %s", primaryCPUQuery)
	}
}

func TestCollectDBReplicaLayers_replicaResourceQueriesUseReplicaContainer(t *testing.T) {
	t.Parallel()
	ns := "cloud9-replica-container-ns"
	replicaPod := "postgres-replica-metrics"
	desired := int32(1)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaDeploymentName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  dbReplicaContainerName,
					Image: "postgres:16",
				}}},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaPod,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(deployment, pod)

	var replicaCPUQuery, replicaMemQuery, replicaCPULimitQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "container_cpu_usage_seconds_total") && strings.Contains(q, replicaPod):
			replicaCPUQuery = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.12"))
		case strings.Contains(q, "container_memory_working_set_bytes") && strings.Contains(q, replicaPod):
			replicaMemQuery = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("67108864"))
		case strings.Contains(q, "kube_pod_container_resource_limits") && strings.Contains(q, `resource="cpu"`) && strings.Contains(q, replicaPod):
			replicaCPULimitQuery = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.5"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns)
	if len(split.replicas) != 1 {
		t.Fatalf("replicas: got %d want 1", len(split.replicas))
	}
	if replicaCPUQuery == "" || replicaMemQuery == "" || replicaCPULimitQuery == "" {
		t.Fatalf("expected replica resource PromQL queries; cpu=%q mem=%q limit=%q", replicaCPUQuery, replicaMemQuery, replicaCPULimitQuery)
	}
	wantContainer := `container="` + dbReplicaContainerName + `"`
	for _, q := range []string{replicaCPUQuery, replicaMemQuery, replicaCPULimitQuery} {
		if !strings.Contains(q, wantContainer) {
			t.Fatalf("replica resource query should use %s: %s", wantContainer, q)
		}
	}
	if split.replicas[0].CPUIsEstimated {
		t.Fatal("replica CPU should be measured, not estimated")
	}
	if split.replicas[0].MemIsEstimated {
		t.Fatal("replica memory should be measured, not estimated")
	}
}

func TestCollectDBReplicaLayers_layerRequestRates(t *testing.T) {
	t.Parallel()
	ns := "cloud9-layer-rps-ns"
	replicaPod := "postgres-replica-rps"
	primaryPod := "postgres-primary"
	desired := int32(1)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaDeploymentName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaPod,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(deployment, pod)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "pg_stat_database_xact_commit"):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				primaryPod: "20",
				replicaPod: "4",
			}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns)
	if split.primary.RequestRate != 20 {
		t.Fatalf("primary requestRate: got %v, want 20", split.primary.RequestRate)
	}
	if len(split.replicas) != 1 || split.replicas[0].RequestRate != 4 {
		t.Fatalf("replica requestRate: %+v", split.replicas)
	}
}

func TestApplyDBReplicaLayers_noReplicas(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{Component: "db", StorageRole: StorageRolePrimary}
	applyDBReplicaLayers(&cm, dbReplicaLayerSplit{})
	if len(cm.Layers) != 0 {
		t.Fatalf("layers: want none, got %d", len(cm.Layers))
	}
	if cm.BackendReplicas != nil {
		t.Fatalf("backendReplicas: want nil, got %+v", cm.BackendReplicas)
	}
}

func TestApplyDBReplicaLayers_withReplicas(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{Component: "db", StorageRole: StorageRolePrimary}
	split := dbReplicaLayerSplit{
		replicaPods: []string{"postgres-replica-abc"},
		counts:      BackendReplicas{Desired: 1, Ready: 1},
		primary: ComponentLayerMetrics{
			ID:     dbLayerIDPostgresPrimary,
			Role:   dbLayerRolePrimary,
			Engine: dbLayerEnginePostgreSQL,
		},
		replicas: []ComponentLayerMetrics{{
			ID:                    "postgres-replica-abc",
			Role:                  dbLayerRoleReplica,
			Engine:                dbLayerEnginePostgreSQL,
			ReplicationLagSeconds: float64Ptr(0.42),
		}},
	}
	applyDBReplicaLayers(&cm, split)
	if len(cm.Layers) != 2 {
		t.Fatalf("layers: want 2, got %d", len(cm.Layers))
	}
	if cm.Layers[0].Role != dbLayerRolePrimary {
		t.Fatalf("primary layer: %+v", cm.Layers[0])
	}
	replica := cm.Layers[1]
	if replica.Role != dbLayerRoleReplica || replicaLagSeconds(replica) != 0.42 {
		t.Fatalf("replica layer: %+v", replica)
	}
	if cm.BackendReplicas == nil || cm.BackendReplicas.Desired != 1 || cm.BackendReplicas.Ready != 1 {
		t.Fatalf("backendReplicas: %+v", cm.BackendReplicas)
	}
}

func TestCollectDBReplicaLayers_zeroReplicaPods(t *testing.T) {
	t.Parallel()
	clientset := fake.NewSimpleClientset()
	c := NewMetricsCollector("", nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), "cloud9-ns")
	if len(split.replicaPods) != 0 {
		t.Fatalf("replicaPods: want none, got %v", split.replicaPods)
	}
}

func TestCollectDBReplicaLayers_lagFromPrimaryCustomQuery(t *testing.T) {
	t.Parallel()
	ns := "cloud9-replica-ns"
	replicaPod := "postgres-replica-7d8f9"
	desired := int32(1)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaDeploymentName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaPod,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	clientset := fake.NewSimpleClientset(deployment, pod)

	var lagQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, postgresExporterReplayLagMetric):
			lagQuery = q
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("application_name", map[string]string{
				replicaPod: "1.25",
			}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns)
	if replicaLagSeconds(split.replicas[0]) != 1.25 {
		t.Fatalf("lag: got %v, want 1.25", replicaLagSeconds(split.replicas[0]))
	}
	if lagQuery == "" {
		t.Fatal("expected primary replay lag PromQL query")
	}
	if !strings.Contains(lagQuery, `namespace="cloud9-replica-ns"`) {
		t.Fatalf("lag query missing namespace: %s", lagQuery)
	}
	if !strings.Contains(lagQuery, `pod!~"postgres-replica.*"`) {
		t.Fatalf("lag query should exclude replica pods on primary scrape: %s", lagQuery)
	}
}

func TestCollectDBReplicaLayers_fallbackWhenCustomSeriesMissing(t *testing.T) {
	t.Parallel()
	ns := "cloud9-fallback-ns"
	replicaPod := "postgres-replica-fb"
	desired := int32(1)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: dbReplicaDeploymentName, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaPod,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(deployment, pod)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, postgresExporterReplicaLagFallbackMetric) && strings.Contains(q, "postgres-replica") {
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{replicaPod: "0.75"}))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns)
	if replicaLagSeconds(split.replicas[0]) != 0.75 {
		t.Fatalf("fallback lag: got %v, want 0.75", replicaLagSeconds(split.replicas[0]))
	}
}

func TestApplyStorageProfiles_dbPrimaryWithReplicas(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"db": {
				Component: "db",
				Layers: []ComponentLayerMetrics{{
					ID:                    "postgres-replica-xyz",
					Role:                  dbLayerRoleReplica,
					ReplicationLagSeconds: float64Ptr(0.5),
				}},
			},
		},
	}
	topo := TopologyFlags{HasDB: true, DBEngine: "postgresql", DBRole: "primary"}
	applyStorageProfiles(dm, topo)
	db := dm.Components["db"]
	if db.StorageRole != StorageRolePrimary {
		t.Fatalf("storageRole: want primary, got %q", db.StorageRole)
	}
}

func TestAnnotateFreshness_DBReplicaLayerReplication(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"db": {
				Component: "db",
				Layers: []ComponentLayerMetrics{
					{ID: dbLayerIDPostgresPrimary, Role: dbLayerRolePrimary},
					{ID: "postgres-replica-1", Role: dbLayerRoleReplica, ReplicationLagSeconds: float64Ptr(0.1)},
				},
			},
		},
	}
	annotateFreshness(dm)
	replica := dm.Components["db"].Layers[1]
	repFresh, ok := replica.Freshness[SignalReplication]
	if !ok {
		t.Fatal("expected replication freshness on replica layer")
	}
	if repFresh.Tier != FreshnessRate || repFresh.Source != "prometheus_gauge" {
		t.Fatalf("replication freshness: %+v", repFresh)
	}
	if _, ok := dm.Components["db"].Layers[0].Freshness[SignalReplication]; ok {
		t.Fatal("primary layer should not have replication freshness")
	}
}

func TestCollect_StorageProfileWithReplicaLayers(t *testing.T) {
	t.Parallel()
	ns := "ns-storage-replica"
	replicaPod := "postgres-replica-roll"
	desired := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaDeploymentName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "postgres",
					Image: "postgres:16",
					Ports: []corev1.ContainerPort{{ContainerPort: 5432, Protocol: corev1.ProtocolTCP}},
				}}},
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 0},
					MaxSurge:       &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
				},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaPod,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(deployment, pod)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, postgresExporterReplayLagMetric) {
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("application_name", map[string]string{replicaPod: "0.05"}))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	topo := TopologyFlags{HasDB: true, DBEngine: "postgresql", DBRole: "primary"}
	dm, err := c.Collect(context.Background(), ns, "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	db := dm.Components["db"]
	if db.StorageRole != StorageRolePrimary {
		t.Fatalf("storageRole: want primary, got %q", db.StorageRole)
	}
	if len(db.Layers) != 2 {
		t.Fatalf("layers: want primary+replica, got %d", len(db.Layers))
	}
	if replicaLagSeconds(db.Layers[1]) != 0.05 {
		t.Fatalf("replica layer: %+v", db.Layers[1])
	}
}
