package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func float64Ptr(v float64) *float64 {
	return &v
}

type fakeReplicationAdmin struct {
	exec func(ctx context.Context, query string) (string, error)
}

func (f *fakeReplicationAdmin) ExecSQL(ctx context.Context, query string) (string, error) {
	if f == nil || f.exec == nil {
		return "", errors.New("fakeReplicationAdmin: no exec hook")
	}
	return f.exec(ctx, query)
}

func fakeDBReplicaStatefulSet(ns string, desired int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
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
}

func replicaLagSeconds(layer ComponentLayerMetrics) float64 {
	if layer.ReplicationLagSeconds == nil {
		return -1
	}
	return *layer.ReplicationLagSeconds
}

func TestPostgresPrimaryReplayLagByAppQuery(t *testing.T) {
	t.Parallel()
	writer := "postgres-replica-0"
	q := postgresPrimaryReplayLagByAppQuery("ns-replica", writer)
	for _, want := range []string{
		`namespace="ns-replica"`,
		`pod="postgres-replica-0"`,
		postgresExporterReplayLagMetric,
		"application_name",
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in query: %s", want, q)
		}
	}
}

func TestPostgresPrimaryWalLsnBytesQuery(t *testing.T) {
	t.Parallel()
	q := postgresPrimaryWalLsnBytesQuery("ns-lsn", "postgres-primary-live")
	for _, want := range []string{
		`namespace="ns-lsn"`,
		`pod="postgres-primary-live"`,
		postgresExporterWalLsnBytesMetric,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in query: %s", want, q)
		}
	}
	if strings.Contains(q, postgresExporterReplayLsnBytesMetric) {
		t.Fatalf("primary LSN query must not scrape replay LSN: %s", q)
	}
}

func TestPostgresReplicaReplayLsnBytesByPodQuery(t *testing.T) {
	t.Parallel()
	q := postgresReplicaReplayLsnBytesByPodQuery("ns-replay-lsn")
	for _, want := range []string{
		`namespace="ns-replay-lsn"`,
		`pod=~"postgres-replica.*"`,
		postgresExporterReplayLsnBytesMetric,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in query: %s", want, q)
		}
	}
}

func TestWalLsnDiffBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		primary, replay float64
		want            int64
	}{
		{name: "climbing", primary: 20000, replay: 4096, want: 15904},
		{name: "caught up", primary: 100, replay: 100, want: 0},
		{name: "replica ahead clamps to zero", primary: 10, replay: 50, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := walLsnDiffBytes(tt.primary, tt.replay); got != tt.want {
				t.Fatalf("walLsnDiffBytes(%v, %v) = %d, want %d", tt.primary, tt.replay, got, tt.want)
			}
		})
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
	}, "postgres-abc")
	if got[dbLayerIDPostgresPrimary] != 12 {
		t.Fatalf("primary rate: got %v, want 12", got[dbLayerIDPostgresPrimary])
	}
	if got["postgres-replica-roll"] != 3 || got["postgres-replica-other"] != 1 {
		t.Fatalf("replica rates: %+v", got)
	}
}

func fakeDBWriteEndpoints(ns, writer string) *corev1.Endpoints {
	return &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: ns},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{
				TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: writer},
			}},
		}},
	}
}

func fakeReplicaPod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func TestListDBReplicaPods_statefulSetDesiredCount(t *testing.T) {
	t.Parallel()
	ns := "cloud9-sts-count-ns"
	desired := int32(2)
	clientset := fake.NewSimpleClientset(
		fakeDBReplicaStatefulSet(ns, desired),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "postgres-replica-0",
				Namespace: ns,
				Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)
	c := NewMetricsCollector("", nil, clientset, nil, nil)
	names, _, counts := c.listDBReplicaPods(context.Background(), ns)
	if counts.Desired != 2 {
		t.Fatalf("desired: got %d want 2", counts.Desired)
	}
	if counts.Ready != 1 {
		t.Fatalf("ready: got %d want 1", counts.Ready)
	}
	if len(names) != 1 || names[0] != "postgres-replica-0" {
		t.Fatalf("names: got %v", names)
	}
}

func TestCollectDBReplicaLayers_primaryResourceQueriesExcludeReplicas(t *testing.T) {
	t.Parallel()
	ns := "cloud9-primary-only-ns"
	writerPod := "postgres-primary-live"
	replicaPod := "postgres-replica-only"
	desired := int32(1)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	clientset := fake.NewSimpleClientset(
		statefulSet,
		fakeReplicaPod(ns, replicaPod),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

	var primaryCPUQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "container_cpu_usage_seconds_total") && strings.Contains(q, writerPod):
			primaryCPUQuery = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.25"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	_ = c.collectDBReplicaLayers(context.Background(), ns, nil)
	if primaryCPUQuery == "" {
		t.Fatal("expected primary CPU PromQL query")
	}
	if !strings.Contains(primaryCPUQuery, `pod=~"^`+writerPod+`$"`) {
		t.Fatalf("primary CPU query should target writer pod: %s", primaryCPUQuery)
	}
}

func TestCollectDBReplicaLayers_replicaResourceQueriesUseReplicaContainer(t *testing.T) {
	t.Parallel()
	ns := "cloud9-replica-container-ns"
	replicaPod := "postgres-replica-metrics"
	desired := int32(1)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
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
	writerPod := "postgres-primary-live"
	clientset := fake.NewSimpleClientset(
		statefulSet,
		fakeReplicaPod(ns, replicaPod),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

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
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
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

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	clientset := fake.NewSimpleClientset(
		statefulSet,
		fakeReplicaPod(ns, replicaPod),
		fakeDBWriteEndpoints(ns, primaryPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      primaryPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

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
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
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
		writerPod:   "postgres-primary",
		replicaPods: []string{"postgres-replica-0"},
		counts:      BackendReplicas{Desired: 1, Ready: 1},
		primary: ComponentLayerMetrics{
			ID:     dbLayerIDPostgresPrimary,
			Role:   dbLayerRolePrimary,
			Engine: dbLayerEnginePostgreSQL,
		},
		replicas: []ComponentLayerMetrics{{
			ID:                    "postgres-replica-0",
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
	split := c.collectDBReplicaLayers(context.Background(), "cloud9-ns", nil)
	if len(split.replicaPods) != 0 {
		t.Fatalf("replicaPods: want none, got %v", split.replicaPods)
	}
}

func TestCollectDBReplicaLayers_lagFromPrimaryCustomQuery(t *testing.T) {
	t.Parallel()
	ns := "cloud9-replica-ns"
	replicaPod := "postgres-replica-7d8f9"
	desired := int32(1)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	writerPod := "postgres-primary-live"
	clientset := fake.NewSimpleClientset(
		statefulSet,
		fakeReplicaPod(ns, replicaPod),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

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
	c.replicationAdminForTest = func(namespace, pod, container string) deployer.PostgresAdmin {
		t.Fatal("replication admin should not run when Prom replication_replay_lag is populated")
		return nil
	}
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if replicaLagSeconds(split.replicas[0]) != 1.25 {
		t.Fatalf("lag: got %v, want 1.25", replicaLagSeconds(split.replicas[0]))
	}
	if lagQuery == "" {
		t.Fatal("expected primary replay lag PromQL query")
	}
	if !strings.Contains(lagQuery, `namespace="cloud9-replica-ns"`) {
		t.Fatalf("lag query missing namespace: %s", lagQuery)
	}
	if !strings.Contains(lagQuery, `pod="`+writerPod+`"`) {
		t.Fatalf("lag query should scrape writer pod: %s", lagQuery)
	}
}

func promSyncStateVectorJSON(syncByApp map[string]string) map[string]any {
	result := make([]map[string]any, 0, len(syncByApp))
	for app, syncState := range syncByApp {
		result = append(result, map[string]any{
			"metric": map[string]string{
				"application_name": app,
				"sync_state":       syncState,
			},
			"value": []any{float64(1), "1"},
		})
	}
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result":     result,
		},
	}
}

func TestCollectDBReplicaLayers_disconnectedReplicaSkipsFallbackLag(t *testing.T) {
	t.Parallel()

	const (
		ns           = "cloud9-disconnected-ns"
		writerPod    = "postgres-primary-live"
		disconnected = "postgres-replica-0"
		connected    = "postgres-replica-1"
	)
	desired := int32(2)

	clientset := fake.NewSimpleClientset(
		fakeDBReplicaStatefulSet(ns, desired),
		fakeReplicaPod(ns, disconnected),
		fakeReplicaPod(ns, connected),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, postgresExporterReplayLagMetric) && strings.Contains(q, "sync_state"):
			_ = json.NewEncoder(w).Encode(promSyncStateVectorJSON(map[string]string{
				connected: "async",
			}))
		case strings.Contains(q, postgresExporterReplayLagMetric):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("application_name", map[string]string{
				connected: "0",
			}))
		case strings.Contains(q, postgresExporterReplayLagBytesMetric):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("application_name", map[string]string{
				connected: "0",
			}))
		case strings.Contains(q, postgresExporterReplicaLagFallbackMetric):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				disconnected: "0",
			}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if len(split.replicas) != 2 {
		t.Fatalf("replicas: got %d want 2", len(split.replicas))
	}

	byID := make(map[string]ComponentLayerMetrics, len(split.replicas))
	for _, layer := range split.replicas {
		byID[layer.ID] = layer
	}

	disconnectedLayer := byID[disconnected]
	if disconnectedLayer.ConnectedOnPrimary {
		t.Fatal("replica-0 should be disconnected on primary")
	}
	if disconnectedLayer.ReplicationLagSeconds != nil {
		t.Fatalf("disconnected replica should not use fallback lag seconds: %+v", disconnectedLayer.ReplicationLagSeconds)
	}
	if disconnectedLayer.LagBytes != nil {
		t.Fatalf("disconnected replica should not have lag bytes: %+v", disconnectedLayer.LagBytes)
	}
	if disconnectedLayer.SyncState != "" {
		t.Fatalf("disconnected replica should not have sync state: %q", disconnectedLayer.SyncState)
	}
	if disconnectedLayer.PromotionCandidate {
		t.Fatal("disconnected replica should not be promotion candidate")
	}

	connectedLayer := byID[connected]
	if !connectedLayer.ConnectedOnPrimary {
		t.Fatal("replica-1 should be connected on primary")
	}
	if connectedLayer.ReplicationLagSeconds == nil || *connectedLayer.ReplicationLagSeconds != 0 {
		t.Fatalf("connected replica lag seconds: %+v", connectedLayer.ReplicationLagSeconds)
	}
	if connectedLayer.LagBytes == nil || *connectedLayer.LagBytes != 0 {
		t.Fatalf("connected replica lag bytes: %+v", connectedLayer.LagBytes)
	}
	if !connectedLayer.PromotionCandidate {
		t.Fatal("connected replica with 0 B lag should be promotion candidate")
	}
}

func TestCollectDBReplicaLayers_disconnectedReplicaUsesLsnDiff(t *testing.T) {
	t.Parallel()

	const (
		ns           = "cloud9-lsn-ns"
		writerPod    = "postgres-primary-live"
		disconnected = "postgres-replica-0"
		connected    = "postgres-replica-1"
	)
	desired := int32(2)

	clientset := fake.NewSimpleClientset(
		fakeDBReplicaStatefulSet(ns, desired),
		fakeReplicaPod(ns, disconnected),
		fakeReplicaPod(ns, connected),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

	var primaryLsnQuery, replayLsnQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, postgresExporterReplayLagMetric) && strings.Contains(q, "sync_state"):
			_ = json.NewEncoder(w).Encode(promSyncStateVectorJSON(map[string]string{
				connected: "async",
			}))
		case strings.Contains(q, postgresExporterReplayLagMetric):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("application_name", map[string]string{
				connected: "0",
			}))
		case strings.Contains(q, postgresExporterReplayLagBytesMetric):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("application_name", map[string]string{
				connected: "0",
			}))
		case strings.Contains(q, postgresExporterReplayLsnBytesMetric):
			replayLsnQuery = q
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				disconnected: "4096",
				connected:    "100000",
			}))
		case strings.Contains(q, postgresExporterWalLsnBytesMetric):
			primaryLsnQuery = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("20000"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if len(split.replicas) != 2 {
		t.Fatalf("replicas: got %d want 2", len(split.replicas))
	}

	byID := make(map[string]ComponentLayerMetrics, len(split.replicas))
	for _, layer := range split.replicas {
		byID[layer.ID] = layer
	}

	disconnectedLayer := byID[disconnected]
	if disconnectedLayer.ConnectedOnPrimary {
		t.Fatal("replica-0 should be disconnected on primary")
	}
	if disconnectedLayer.ReplicationLagSeconds != nil {
		t.Fatalf("disconnected replica must not use fallback lag seconds: %+v", disconnectedLayer.ReplicationLagSeconds)
	}
	if disconnectedLayer.LagBytes == nil || *disconnectedLayer.LagBytes != 15904 {
		t.Fatalf("disconnected replica LSN diff lag: %+v", disconnectedLayer.LagBytes)
	}
	if disconnectedLayer.SyncState != "" {
		t.Fatalf("disconnected replica should not have sync state: %q", disconnectedLayer.SyncState)
	}
	if disconnectedLayer.PromotionCandidate {
		t.Fatal("disconnected replica should not be promotion candidate")
	}

	connectedLayer := byID[connected]
	if !connectedLayer.ConnectedOnPrimary {
		t.Fatal("replica-1 should be connected on primary")
	}
	if connectedLayer.LagBytes == nil || *connectedLayer.LagBytes != 0 {
		t.Fatalf("connected replica must keep primary lag bytes, not LSN diff: %+v", connectedLayer.LagBytes)
	}
	if !connectedLayer.PromotionCandidate {
		t.Fatal("connected replica with 0 B lag should be promotion candidate")
	}

	if primaryLsnQuery == "" || replayLsnQuery == "" {
		t.Fatal("expected primary and replica LSN PromQL queries")
	}
	if !strings.Contains(primaryLsnQuery, `namespace="cloud9-lsn-ns"`) {
		t.Fatalf("primary LSN query missing namespace: %s", primaryLsnQuery)
	}
	if !strings.Contains(primaryLsnQuery, `pod="`+writerPod+`"`) {
		t.Fatalf("primary LSN query should scrape writer pod: %s", primaryLsnQuery)
	}
	if !strings.Contains(replayLsnQuery, `namespace="cloud9-lsn-ns"`) {
		t.Fatalf("replay LSN query missing namespace: %s", replayLsnQuery)
	}
}

func TestCollectDBReplicaLayers_fallbackWhenCustomSeriesMissing(t *testing.T) {
	t.Parallel()
	ns := "cloud9-fallback-ns"
	replicaPod := "postgres-replica-0"
	desired := int32(1)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	writerPod := "postgres-primary-live"
	clientset := fake.NewSimpleClientset(
		statefulSet,
		fakeReplicaPod(ns, replicaPod),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

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
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if split.replicas[0].ConnectedOnPrimary {
		t.Fatal("replica without primary row should be disconnected on primary")
	}
	if split.replicas[0].ReplicationLagSeconds != nil {
		t.Fatalf("disconnected replica should not use standby fallback lag: %+v", split.replicas[0].ReplicationLagSeconds)
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
	replicaPod := "postgres-replica-0"
	desired := int32(1)
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
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
		},
	}
	writerPod := "postgres-primary-live"
	clientset := fake.NewSimpleClientset(
		statefulSet,
		fakeReplicaPod(ns, replicaPod),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

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

func TestPostgresXactTotalByPodQuery(t *testing.T) {
	t.Parallel()
	q := postgresXactTotalByPodQuery("ns-xact-total")
	for _, want := range []string{
		`namespace="ns-xact-total"`,
		`sum by (pod)`,
		`pg_stat_database_xact_commit`,
		`pg_stat_database_xact_rollback`,
		`datname!=""`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in query: %s", want, q)
		}
	}
	if strings.Contains(q, "rate(") {
		t.Fatalf("xact total query should not use rate(): %s", q)
	}
}

func TestPostgresReplicaUserTableReadRateQuery_targetsReplicaPod(t *testing.T) {
	t.Parallel()
	replicaPod := "postgres-replica-scan"
	q := postgresReplicaUserTableReadRateQuery("ns-scan", replicaPod)
	for _, want := range []string{
		`namespace="ns-scan"`,
		`pod=~"^postgres-replica-scan$"`,
		`pg_stat_user_tables_seq_tup_read`,
		`pg_stat_user_tables_idx_tup_fetch`,
		`relname!~"_c9_.*"`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in query: %s", want, q)
		}
	}
	if strings.Contains(q, `pod!~"postgres-replica.*"`) {
		t.Fatalf("replica scan query should target replica pod, not exclude replicas: %s", q)
	}
}

func TestCollectDBReplicaLayers_engineMetrics(t *testing.T) {
	t.Parallel()
	ns := "cloud9-engine-layers-ns"
	replicaPod := "postgres-replica-engine"
	primaryPod := "postgres-primary-engine"
	desired := int32(1)
	clusterIO := &StorageIO{
		Unit:         "rows/s",
		Source:       "postgres_exporter",
		ReadRate:     100,
		WriteRate:    40,
		ReadTotal9m:  5000,
		WriteTotal9m: 900,
		ReadLabel:    "Rows scanned/s",
		WriteLabel:   "Rows written/s",
	}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	clientset := fake.NewSimpleClientset(
		statefulSet,
		fakeReplicaPod(ns, replicaPod),
		fakeDBWriteEndpoints(ns, primaryPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      primaryPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

	var replicaScanQuery string
	xactTotals := map[string]string{
		primaryPod: "1000",
		replicaPod: "200",
	}
	readTotal := "800"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "pg_stat_database_xact_commit") && !strings.Contains(q, "rate("):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", xactTotals))
		case strings.Contains(q, "pg_stat_database_numbackends"):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				primaryPod: "75",
				replicaPod: "4",
			}))
		case strings.Contains(q, "pg_stat_activity_count"):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				primaryPod: "3",
				replicaPod: "1",
			}))
		case strings.Contains(q, "pg_stat_user_tables_seq_tup_read") && strings.Contains(q, replicaPod):
			replicaScanQuery = q
			if strings.Contains(q, "rate(") {
				_ = json.NewEncoder(w).Encode(promVectorJSON("12.5"))
				return
			}
			_ = json.NewEncoder(w).Encode(promVectorJSON(readTotal))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	_ = c.collectDBReplicaLayers(context.Background(), ns, clusterIO)
	split := c.collectDBReplicaLayers(context.Background(), ns, clusterIO)

	if replicaScanQuery == "" {
		t.Fatal("expected replica user-table scan PromQL query")
	}
	if !strings.Contains(replicaScanQuery, `pod=~"^`+replicaPod+`$"`) {
		t.Fatalf("replica scan query should target replica pod: %s", replicaScanQuery)
	}

	if split.primary.StorageIO == nil {
		t.Fatal("primary layer should copy cluster storageIo")
	}
	if split.primary.StorageIO.ReadRate != 100 || split.primary.StorageIO.WriteRate != 40 {
		t.Fatalf("primary storageIo: %+v", split.primary.StorageIO)
	}
	if split.primary.DbActiveConnections != 75 {
		t.Fatalf("primary connections: got %v want 75", split.primary.DbActiveConnections)
	}

	if len(split.replicas) != 1 {
		t.Fatalf("replicas: got %d want 1", len(split.replicas))
	}
	replica := split.replicas[0]
	if replica.DbActiveConnections != 4 {
		t.Fatalf("replica connections: got %v want 4", replica.DbActiveConnections)
	}
	if replica.StorageIO == nil {
		t.Fatal("replica layer should have read-only storageIo")
	}
	if replica.StorageIO.ReadRate != 12.5 {
		t.Fatalf("replica readRate: got %v want 12.5", replica.StorageIO.ReadRate)
	}
	if replica.StorageIO.WriteRate != 0 || replica.StorageIO.WriteTotal9m != 0 {
		t.Fatalf("replica layer should omit write I/O: %+v", replica.StorageIO)
	}

	// Baseline scrape then unchanged counters → session totals stay 0.
	if split.replicas[0].TotalRequests != 0 {
		t.Fatalf("replica xact session after baseline with stable counter: got %v want 0", split.replicas[0].TotalRequests)
	}
	if split.replicas[0].StorageIO.ReadTotal9m != 0 {
		t.Fatalf("replica read session after baseline with stable counter: got %v want 0", split.replicas[0].StorageIO.ReadTotal9m)
	}

	xactTotals[replicaPod] = "250"
	readTotal = "850"
	split2 := c.collectDBReplicaLayers(context.Background(), ns, clusterIO)
	if split2.replicas[0].TotalRequests != 50 {
		t.Fatalf("replica xact session delta: got %v want 50", split2.replicas[0].TotalRequests)
	}
	if split2.replicas[0].StorageIO.ReadTotal9m != 50 {
		t.Fatalf("replica read session delta: got %v want 50", split2.replicas[0].StorageIO.ReadTotal9m)
	}
}

func TestCollectDBReplicaLayers_xactSessionRebaselineOnCounterDrop(t *testing.T) {
	t.Parallel()
	ns := "cloud9-xact-rebaseline-ns"
	replicaPod := "postgres-replica-0"
	desired := int32(1)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	writerPod := "postgres-primary-live"
	clientset := fake.NewSimpleClientset(
		statefulSet,
		fakeReplicaPod(ns, replicaPod),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

	xactTotal := "500"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "pg_stat_database_xact_commit") && !strings.Contains(q, "rate(") {
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				replicaPod: xactTotal,
			}))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	_ = c.collectDBReplicaLayers(context.Background(), ns, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if split.replicas[0].TotalRequests != 0 {
		t.Fatalf("after baseline+delta expect 0 delta on stable counter, got %v", split.replicas[0].TotalRequests)
	}

	xactTotal = "50" // pod restart counter drop
	split = c.collectDBReplicaLayers(context.Background(), ns, nil)
	if split.replicas[0].TotalRequests != 50 {
		t.Fatalf("counter drop should rebaseline session total to now, got %v want 50", split.replicas[0].TotalRequests)
	}
}

func TestCollectDBReplicaLayers_postgresSessionHeldSkipsLayerTotals(t *testing.T) {
	t.Parallel()
	ns := "cloud9-pg-held-ns"
	replicaPod := "postgres-replica-0"
	desired := int32(1)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaStatefulSetName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	writerPod := "postgres-primary-live"
	clientset := fake.NewSimpleClientset(
		statefulSet,
		fakeReplicaPod(ns, replicaPod),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "pg_stat_database_xact_commit") && !strings.Contains(q, "rate(") {
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{replicaPod: "999"}))
			return
		}
		if strings.Contains(q, "pg_stat_user_tables_seq_tup_read") && strings.Contains(q, replicaPod) && !strings.Contains(q, "rate(") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("777"))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	c.HoldPostgresSession(ns)
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if split.primary.TotalRequests != 0 {
		t.Fatalf("held session should skip primary xact totals, got %v", split.primary.TotalRequests)
	}
	if split.replicas[0].TotalRequests != 0 {
		t.Fatalf("held session should skip replica xact totals, got %v", split.replicas[0].TotalRequests)
	}
	if split.replicas[0].StorageIO == nil || split.replicas[0].StorageIO.ReadTotal9m != 0 {
		t.Fatalf("held session should skip replica read totals, got %+v", split.replicas[0].StorageIO)
	}
}

func TestCollectDBReplicaLayers_postPromoteWriterExcludedFromReplicas(t *testing.T) {
	t.Parallel()
	ns := "cloud9-post-promote"
	writerPod := "postgres-replica-0"
	standbyPod := "postgres-replica-1"
	desired := int32(2)
	zero := int32(0)

	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: ns},
			Spec:       appsv1.DeploymentSpec{Replicas: &zero},
		},
		fakeDBReplicaStatefulSet(ns, desired),
		fakeReplicaPod(ns, writerPod),
		fakeReplicaPod(ns, standbyPod),
		fakeDBWriteEndpoints(ns, writerPod),
	)

	c := NewMetricsCollector("", nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if split.writerPod != writerPod {
		t.Fatalf("writerPod: got %q want %q", split.writerPod, writerPod)
	}
	if len(split.replicaPods) != 1 || split.replicaPods[0] != standbyPod {
		t.Fatalf("standbys: got %v want [%s]", split.replicaPods, standbyPod)
	}
	if split.counts.Ready != 1 {
		t.Fatalf("ready standbys: got %d want 1", split.counts.Ready)
	}

	cm := ComponentMetrics{Component: "db"}
	applyDBReplicaLayers(&cm, split)
	if len(cm.Layers) != 2 {
		t.Fatalf("layers: got %d want 2", len(cm.Layers))
	}
	if cm.Layers[0].ID != dbLayerIDPostgresPrimary {
		t.Fatalf("primary layer id: %+v", cm.Layers[0])
	}
	if cm.Layers[1].ID != standbyPod {
		t.Fatalf("standby layer id: %+v", cm.Layers[1])
	}
}

func TestCollectDBReplicaLayers_prePromoteThreeLayers(t *testing.T) {
	t.Parallel()
	ns := "cloud9-pre-promote"
	writerPod := "postgres-abc123"
	standby0 := "postgres-replica-0"
	standby1 := "postgres-replica-1"
	desired := int32(2)
	one := int32(1)

	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: ns},
			Spec:       appsv1.DeploymentSpec{Replicas: &one},
		},
		fakeDBReplicaStatefulSet(ns, desired),
		fakeReplicaPod(ns, standby0),
		fakeReplicaPod(ns, standby1),
		fakeDBWriteEndpoints(ns, writerPod),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writerPod,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)

	c := NewMetricsCollector("", nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if len(split.replicaPods) != 2 {
		t.Fatalf("standbys: got %v want two", split.replicaPods)
	}
	if split.counts.Ready != 2 {
		t.Fatalf("ready standbys: got %d want 2", split.counts.Ready)
	}

	cm := ComponentMetrics{Component: "db"}
	applyDBReplicaLayers(&cm, split)
	if len(cm.Layers) != 3 {
		t.Fatalf("layers: got %d want 3", len(cm.Layers))
	}
	if cm.Layers[0].ID != dbLayerIDPostgresPrimary {
		t.Fatalf("primary layer: %+v", cm.Layers[0])
	}
}

func splitToReplicationSnapshot(split dbReplicaLayerSplit, leftover string) primaryReplicationSnapshot {
	snap := newPrimaryReplicationSnapshot(split.replicaPods)
	for podName, connected := range split.connectedOnPrimary {
		snap.connected[podName] = connected
	}
	if layer, ok := findReplicaLayer(split.replicas, leftover); ok {
		if layer.ReplicationLagSeconds != nil {
			snap.lagSeconds[leftover] = *layer.ReplicationLagSeconds
		}
		if layer.LagBytes != nil {
			snap.lagBytes[leftover] = *layer.LagBytes
		}
		if layer.SyncState != "" {
			snap.syncState[leftover] = layer.SyncState
		}
	}
	return snap
}

func findReplicaLayer(layers []ComponentLayerMetrics, id string) (ComponentLayerMetrics, bool) {
	for _, layer := range layers {
		if layer.ID == id {
			return layer, true
		}
	}
	return ComponentLayerMetrics{}, false
}

func TestParseReplicationStateRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		stdout string
		want   map[string]struct {
			connected bool
			sync      string
			seconds   float64
			bytes     int64
		}
		missing []string
	}{
		{
			name:   "streaming quorum row",
			stdout: "postgres-replica-0|streaming|quorum|0|0\n",
			want: map[string]struct {
				connected bool
				sync      string
				seconds   float64
				bytes     int64
			}{
				"postgres-replica-0": {connected: true, sync: "quorum", seconds: 0, bytes: 0},
			},
		},
		{
			name:   "skip malformed line",
			stdout: "bad|row\npostgres-replica-1|streaming|async|1.5|42\n",
			want: map[string]struct {
				connected bool
				sync      string
				seconds   float64
				bytes     int64
			}{
				"postgres-replica-1": {connected: true, sync: "async", seconds: 1.5, bytes: 42},
			},
			missing: []string{"bad"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			connected, lagSeconds, lagBytes, syncState := parseReplicationStateRows(tt.stdout)
			for app, want := range tt.want {
				if !connected[app] || !want.connected {
					t.Fatalf("connected[%s]: got %v want %v", app, connected[app], want.connected)
				}
				if syncState[app] != want.sync {
					t.Fatalf("syncState[%s]: got %q want %q", app, syncState[app], want.sync)
				}
				if lagSeconds[app] != want.seconds {
					t.Fatalf("lagSeconds[%s]: got %v want %v", app, lagSeconds[app], want.seconds)
				}
				if lagBytes[app] != want.bytes {
					t.Fatalf("lagBytes[%s]: got %v want %v", app, lagBytes[app], want.bytes)
				}
			}
			for _, app := range tt.missing {
				if connected[app] {
					t.Fatalf("unexpected connected[%s]", app)
				}
			}
		})
	}
}

func TestCollectDBReplicaLayers_sqlFallbackOnPromotedWriter(t *testing.T) {
	t.Parallel()

	const (
		ns        = "cloud9-sql-fallback-ns"
		writerPod = "postgres-replica-1"
		leftover  = "postgres-replica-0"
	)
	desired := int32(2)
	zero := int32(0)

	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: ns},
			Spec:       appsv1.DeploymentSpec{Replicas: &zero},
		},
		fakeDBReplicaStatefulSet(ns, desired),
		fakeReplicaPod(ns, writerPod),
		fakeReplicaPod(ns, leftover),
		fakeDBWriteEndpoints(ns, writerPod),
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	c.replicationAdminForTest = func(namespace, pod, container string) deployer.PostgresAdmin {
		return &fakeReplicationAdmin{exec: func(ctx context.Context, query string) (string, error) {
			if strings.Contains(query, "pg_stat_replication") {
				return leftover + "|streaming|quorum|0|0", nil
			}
			if strings.Contains(query, "0/0") {
				return "100", nil
			}
			return "", errors.New("unexpected query: " + query)
		}}
	}

	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if len(split.replicas) != 1 || split.replicas[0].ID != leftover {
		t.Fatalf("standbys: %+v", split.replicas)
	}
	layer := split.replicas[0]
	if !layer.ConnectedOnPrimary {
		t.Fatal("leftover should be connected via SQL fallback")
	}
	if layer.SyncState != "quorum" {
		t.Fatalf("syncState: got %q want quorum", layer.SyncState)
	}
	if layer.ReplicationLagSeconds == nil || *layer.ReplicationLagSeconds != 0 {
		t.Fatalf("lag seconds: %+v", layer.ReplicationLagSeconds)
	}
	if layer.LagBytes == nil || *layer.LagBytes != 0 {
		t.Fatalf("lag bytes: %+v", layer.LagBytes)
	}
}

func TestCollectDBReplicaLayers_sqlTimeoutUsesLastGoodCache(t *testing.T) {
	t.Parallel()

	const (
		ns        = "cloud9-sql-timeout-ns"
		writerPod = "postgres-replica-1"
		leftover  = "postgres-replica-0"
	)
	desired := int32(2)
	zero := int32(0)
	slow := false

	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: ns},
			Spec:       appsv1.DeploymentSpec{Replicas: &zero},
		},
		fakeDBReplicaStatefulSet(ns, desired),
		fakeReplicaPod(ns, writerPod),
		fakeReplicaPod(ns, leftover),
		fakeDBWriteEndpoints(ns, writerPod),
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	c.replicationAdminForTest = func(namespace, pod, container string) deployer.PostgresAdmin {
		return &fakeReplicationAdmin{exec: func(ctx context.Context, query string) (string, error) {
			if slow {
				if ctx.Err() != nil {
					return "", ctx.Err()
				}
				time.Sleep(3 * time.Second)
				return "", context.DeadlineExceeded
			}
			if strings.Contains(query, "pg_stat_replication") {
				return leftover + "|streaming|quorum|0|0", nil
			}
			if strings.Contains(query, "0/0") {
				return "100", nil
			}
			return "", errors.New("unexpected query")
		}}
	}

	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if len(split.replicas) != 1 || !split.replicas[0].ConnectedOnPrimary {
		t.Fatalf("first collect should connect leftover via SQL: %+v", split.replicas)
	}

	c.replSQLMu.Lock()
	if c.replSQLCache == nil {
		c.replSQLCache = make(map[string]replSQLCacheEntry)
	}
	c.replSQLCache[ns] = replSQLCacheEntry{
		snap:      copyPrimaryReplicationSnapshot(splitToReplicationSnapshot(split, leftover)),
		expiresAt: time.Now().Add(-time.Second),
	}
	c.replSQLMu.Unlock()

	slow = true
	split2 := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if len(split2.replicas) != 1 || !split2.replicas[0].ConnectedOnPrimary {
		t.Fatalf("expired cache after SQL timeout should keep connected leftover: %+v", split2.replicas)
	}

	c2 := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	c2.replicationAdminForTest = c.replicationAdminForTest
	slow = true
	split3 := c2.collectDBReplicaLayers(context.Background(), ns, nil)
	if len(split3.replicas) != 1 {
		t.Fatalf("replicas: %+v", split3.replicas)
	}
	if split3.replicas[0].ConnectedOnPrimary {
		t.Fatal("first collect with no cache and SQL error should leave leftover disconnected")
	}
}

func TestCollectDBReplicaLayers_climbingLagWithoutReplicationRow(t *testing.T) {
	t.Parallel()

	const (
		ns        = "cloud9-climb-no-row-ns"
		writerPod = "postgres-replica-1"
		leftover  = "postgres-replica-0"
	)
	desired := int32(2)
	zero := int32(0)

	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: ns},
			Spec:       appsv1.DeploymentSpec{Replicas: &zero},
		},
		fakeDBReplicaStatefulSet(ns, desired),
		fakeReplicaPod(ns, writerPod),
		fakeReplicaPod(ns, leftover),
		fakeDBWriteEndpoints(ns, writerPod),
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, postgresExporterReplayLsnBytesMetric):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				leftover: "4096",
			}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	c.replicationAdminForTest = func(namespace, pod, container string) deployer.PostgresAdmin {
		return &fakeReplicationAdmin{exec: func(ctx context.Context, query string) (string, error) {
			if strings.Contains(query, "pg_stat_replication") {
				return "", nil
			}
			if strings.Contains(query, "0/0") {
				return "20000", nil
			}
			return "", errors.New("unexpected query")
		}}
	}

	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if len(split.replicas) != 1 {
		t.Fatalf("replicas: %+v", split.replicas)
	}
	layer := split.replicas[0]
	if layer.ConnectedOnPrimary {
		t.Fatal("leftover should be disconnected without pg_stat_replication row")
	}
	if layer.LagBytes == nil || *layer.LagBytes != 15904 {
		t.Fatalf("climbing partition lag bytes: %+v", layer.LagBytes)
	}
	if layer.LagBytes != nil && *layer.LagBytes == 0 {
		t.Fatal("partition lag should not be forced to 0 from empty replication")
	}
}

func TestApplyDBReplicaLayers_primaryOnlyAfterSecondPromote(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{Component: "db"}
	split := dbReplicaLayerSplit{
		writerPod: "postgres-replica-0",
		primary: ComponentLayerMetrics{
			ID:     dbLayerIDPostgresPrimary,
			Role:   dbLayerRolePrimary,
			Engine: dbLayerEnginePostgreSQL,
		},
		counts: BackendReplicas{Desired: 2, Ready: 0},
	}
	applyDBReplicaLayers(&cm, split)
	if len(cm.Layers) != 1 {
		t.Fatalf("layers: got %d want 1", len(cm.Layers))
	}
	if cm.BackendReplicas == nil || cm.BackendReplicas.Ready != 0 {
		t.Fatalf("backendReplicas: %+v", cm.BackendReplicas)
	}
}
