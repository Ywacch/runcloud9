package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDbAppSQLRateByTargetQuery(t *testing.T) {
	t.Parallel()
	q := dbAppSQLRateByTargetQuery(`namespace="ns",pod=~"app-server.*"`, "[10s]")
	for _, want := range []string{
		`sum by (target)`,
		`db_query_duration_seconds_count`,
		`pod=~"app-server.*"`,
		`[10s]`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in query: %s", want, q)
		}
	}
}

func TestDbAppSQLTotalByTargetQuery(t *testing.T) {
	t.Parallel()
	q := dbAppSQLTotalByTargetQuery(`namespace="ns",pod=~"app-server.*"`)
	if !strings.Contains(q, `sum by (target) (db_query_duration_seconds_count{`) {
		t.Fatalf("unexpected total query: %s", q)
	}
	if strings.Contains(q, "rate(") || strings.Contains(q, "[9m]") {
		t.Fatalf("total query must be raw counter without rate/window: %s", q)
	}
}

func TestMapAppSQLTargetToLayerID(t *testing.T) {
	t.Parallel()
	known := map[string]struct{}{
		"postgres-replica-0": {},
		"postgres-replica-1": {},
	}
	cases := []struct {
		target string
		layer  string
		ok     bool
	}{
		{"postgres", dbLayerIDPostgresPrimary, true},
		{"", dbLayerIDPostgresPrimary, true},
		{"postgres-replica-0", "postgres-replica-0", true},
		{"postgres-replica-1", "postgres-replica-1", true},
		{"cache-proxy-xyz", "", false},
		{"postgres-replica-9", "", false},
	}
	for _, tc := range cases {
		layer, ok := mapAppSQLTargetToLayerID(tc.target, known)
		if ok != tc.ok || layer != tc.layer {
			t.Fatalf("target %q: got (%q, %v) want (%q, %v)", tc.target, layer, ok, tc.layer, tc.ok)
		}
	}
}

func TestEnrichDBLayerAppSQL_twoTargetsFillLayers(t *testing.T) {
	t.Parallel()
	ns := "cloud9-layer-sql-ns"
	replica0 := "postgres-replica-0"
	replica1 := "postgres-replica-1"
	desired := int32(2)
	writerPod := "postgres-primary-live"
	clientset := fake.NewSimpleClientset(
		fakeDBReplicaStatefulSet(ns, desired),
		fakeReplicaPod(ns, replica0),
		fakeReplicaPod(ns, replica1),
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

	var sessionQuantileQ string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "db_query_duration_seconds_count") && strings.Contains(q, "rate(") && strings.Contains(q, "by (target)"):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("target", map[string]string{
				"postgres":           "12",
				replica0:             "5",
				replica1:             "3",
				"postgres-replica-9": "99",
			}))
		case strings.Contains(q, "db_query_duration_seconds_count") && strings.Contains(q, "by (target)") && !strings.Contains(q, "rate("):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("target", map[string]string{
				"postgres": "120",
				replica0:   "50",
				replica1:   "30",
			}))
		case strings.Contains(q, "db_query_duration_seconds_bucket") && strings.Contains(q, "histogram_quantile(0.99"):
			if strings.Contains(q, "[9m]") {
				t.Fatalf("session SQL quantile must not use fixed [9m]: %s", q)
			}
			if strings.Contains(q, "[1m]") {
				sessionQuantileQ = q
				_ = json.NewEncoder(w).Encode(promMultiVectorJSON("target", map[string]string{
					"postgres": "0.05",
					replica0:   "0.06",
					replica1:   "0.07",
				}))
				return
			}
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("target", map[string]string{
				"postgres": "0.02",
				replica0:   "0.03",
				replica1:   "0.04",
			}))
		case strings.Contains(q, "db_query_duration_seconds_bucket") && strings.Contains(q, "histogram_quantile"):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("target", map[string]string{
				"postgres": "0.02",
				replica0:   "0.03",
				replica1:   "0.04",
			}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	c.sessionAcc.Elapsed(ns, time.Now()) // prime elapsed for session range
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if len(split.replicas) != 2 {
		t.Fatalf("replicas: got %d want 2", len(split.replicas))
	}
	if split.primary.DbQueryRate != 12 {
		t.Fatalf("primary dbQueryRate: got %v want 12", split.primary.DbQueryRate)
	}
	if split.primary.DbTotalQueries != 120 {
		t.Fatalf("primary dbTotalQueries: got %v want 120", split.primary.DbTotalQueries)
	}
	if split.primary.P99Latency != 0.02 {
		t.Fatalf("primary p99: got %v want 0.02", split.primary.P99Latency)
	}
	if split.replicas[0].DbQueryRate != 5 || split.replicas[1].DbQueryRate != 3 {
		t.Fatalf("replica rates: %+v", split.replicas)
	}
	if split.replicas[0].DbTotalQueries != 50 || split.replicas[1].DbTotalQueries != 30 {
		t.Fatalf("replica totals: %+v", split.replicas)
	}
	if sessionQuantileQ == "" {
		t.Fatal("expected session SQL quantile query with clamped [1m] range")
	}
	if !strings.Contains(sessionQuantileQ, `pod=~"app-server.*"`) {
		t.Fatalf("session quantile missing app-server filter: %s", sessionQuantileQ)
	}
}

func TestEnrichDBLayerAppSQL_unlabeledTargetMapsToPrimary(t *testing.T) {
	t.Parallel()
	ns := "cloud9-layer-sql-unlabeled"
	replicaPod := "postgres-replica-0"
	desired := int32(1)
	writerPod := "postgres-primary-live"
	clientset := fake.NewSimpleClientset(
		fakeDBReplicaStatefulSet(ns, desired),
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
		switch {
		case strings.Contains(q, "db_query_duration_seconds_count") && strings.Contains(q, "by (target)"):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("target", map[string]string{
				"": "7",
			}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	split := c.collectDBReplicaLayers(context.Background(), ns, nil)
	if split.primary.DbTotalQueries != 7 {
		t.Fatalf("unlabeled target should map to primary: got %v", split.primary.DbTotalQueries)
	}
	if split.replicas[0].DbTotalQueries != 0 {
		t.Fatalf("replica should stay empty for unlabeled series: got %v", split.replicas[0].DbTotalQueries)
	}
}

func TestCollect_DBLayerAppSQLDoesNotChangeClusterTotals(t *testing.T) {
	t.Parallel()
	ns := "cloud9-layer-sql-cluster"
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

	var clusterRateQ string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "db_query_duration_seconds_count") && strings.Contains(q, "rate(") && strings.Contains(q, "by (target)"):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("target", map[string]string{
				"postgres": "4",
				replicaPod: "2",
			}))
		case strings.Contains(q, "db_query_duration_seconds_count") && strings.Contains(q, "rate(") && !strings.Contains(q, "by (target)"):
			clusterRateQ = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("9"))
		case strings.Contains(q, "db_query_duration_seconds_count") && strings.Contains(q, "by (pod)"):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"app-server-abc": "100",
			}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, clientset, nil, nil)
	dm, err := c.Collect(context.Background(), ns, "ecommerce", TopologyFlags{HasDB: true})
	if err != nil {
		t.Fatal(err)
	}
	db := dm.Components["db"]
	if db.DbQueryRate != 9 {
		t.Fatalf("cluster dbQueryRate should stay unsplit sum: got %v", db.DbQueryRate)
	}
	if strings.Contains(clusterRateQ, "by (target)") {
		t.Fatalf("cluster rate query must not split by target: %s", clusterRateQ)
	}
	if len(db.Layers) != 2 {
		t.Fatalf("layers: got %d want 2", len(db.Layers))
	}
	if db.Layers[0].DbQueryRate != 4 {
		t.Fatalf("primary layer dbQueryRate: got %v want 4", db.Layers[0].DbQueryRate)
	}
	if db.Layers[1].DbQueryRate != 2 {
		t.Fatalf("replica layer dbQueryRate: got %v want 2", db.Layers[1].DbQueryRate)
	}
}
