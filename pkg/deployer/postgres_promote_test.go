package deployer

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPickCandidate_mostCurrent_prefersSyncedLowestLag(t *testing.T) {
	t.Parallel()

	candidates := []PromotionCandidate{
		{PodName: "postgres-replica-1", SyncState: "async", LagBytes: 0},
		{PodName: "postgres-replica-0", SyncState: "sync", LagBytes: 100},
	}
	got, err := PickCandidate(candidates, PreferMostCurrent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PodName != "postgres-replica-0" {
		t.Fatalf("got %+v want sync standby even with higher lag", got)
	}
}

func TestPickCandidate_mostCurrent_lowestLagWhenNoSynced(t *testing.T) {
	t.Parallel()

	candidates := []PromotionCandidate{
		{PodName: "postgres-replica-1", SyncState: "async", LagBytes: 50},
		{PodName: "postgres-replica-0", SyncState: "async", LagBytes: 10},
	}
	got, err := PickCandidate(candidates, PreferMostCurrent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PodName != "postgres-replica-0" || got.LagBytes != 10 {
		t.Fatalf("got %+v", got)
	}
}

func TestPickCandidate_mostCurrent_tieBreakByPodName(t *testing.T) {
	t.Parallel()

	candidates := []PromotionCandidate{
		{PodName: "postgres-replica-1", SyncState: "sync", LagBytes: 0},
		{PodName: "postgres-replica-0", SyncState: "quorum", LagBytes: 0},
	}
	got, err := PickCandidate(candidates, PreferMostCurrent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PodName != "postgres-replica-0" {
		t.Fatalf("got %+v want ascending pod name tie-break", got)
	}
}

func TestPickCandidate_mostLagging_highestLag(t *testing.T) {
	t.Parallel()

	candidates := []PromotionCandidate{
		{PodName: "postgres-replica-0", SyncState: "sync", LagBytes: 10},
		{PodName: "postgres-replica-1", SyncState: "async", LagBytes: 500},
	}
	got, err := PickCandidate(candidates, PreferMostLagging)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PodName != "postgres-replica-1" {
		t.Fatalf("got %+v", got)
	}
}

func TestPickCandidate_noStandbys(t *testing.T) {
	t.Parallel()

	_, err := PickCandidate(nil, PreferMostCurrent)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSelectPromotionCandidate_queriesPrimary(t *testing.T) {
	t.Parallel()

	admin := &fakePromoteAdmin{
		rows: "postgres-replica-1|async|0\npostgres-replica-0|sync|42\n",
	}
	got, err := SelectPromotionCandidate(context.Background(), admin, PreferMostCurrent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PodName != "postgres-replica-0" || got.SyncState != "sync" || got.LagBytes != 42 {
		t.Fatalf("got %+v", got)
	}
	if admin.lastQuery != sqlSelectStreamingStandbys {
		t.Fatalf("query: got %q", admin.lastQuery)
	}
}

func TestSelectPromotionCandidate_noStreamingRows(t *testing.T) {
	t.Parallel()

	admin := &fakePromoteAdmin{rows: ""}
	_, err := SelectPromotionCandidate(context.Background(), admin, PreferMostLagging)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFencePrimary_scalesDeploymentToZero(t *testing.T) {
	t.Parallel()

	ns := "cloud9-test-fence"
	replicas := int32(1)
	client := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: postgresPrimaryDeploymentName, Namespace: ns},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	})

	if err := FencePrimary(context.Background(), client, ns); err != nil {
		t.Fatalf("FencePrimary: %v", err)
	}
	dep, err := client.AppsV1().Deployments(ns).Get(context.Background(), postgresPrimaryDeploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 0 {
		t.Fatalf("replicas: got %v want 0", dep.Spec.Replicas)
	}
}

func TestRetargetWriteService_patchesSelector(t *testing.T) {
	t.Parallel()

	ns := "cloud9-test-retarget"
	podName := "postgres-replica-0"
	client := fake.NewSimpleClientset(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: postgresWriteServiceName, Namespace: ns},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app":        postgresPrimaryAppLabel,
				"deployment": ns,
			},
		},
	})

	if err := RetargetWriteService(context.Background(), client, ns, podName); err != nil {
		t.Fatalf("RetargetWriteService: %v", err)
	}
	svc, err := client.CoreV1().Services(ns).Get(context.Background(), postgresWriteServiceName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	want := map[string]string{
		statefulSetPodNameLabel: podName,
		"deployment":            ns,
	}
	if svc.Spec.Selector["app"] != "" {
		t.Fatalf("selector still has app label: %+v", svc.Spec.Selector)
	}
	for k, v := range want {
		if svc.Spec.Selector[k] != v {
			t.Fatalf("selector[%q]=%q want %q; full %+v", k, svc.Spec.Selector[k], v, svc.Spec.Selector)
		}
	}
}

func TestPromoteStandby_success(t *testing.T) {
	t.Parallel()

	admin := &fakePromoteAdmin{recovery: "f"}
	if err := PromoteStandby(context.Background(), admin); err != nil {
		t.Fatalf("PromoteStandby: %v", err)
	}
	if len(admin.queries) != 2 || admin.queries[0] != sqlPromoteStandby {
		t.Fatalf("queries: %v", admin.queries)
	}
}

func TestPromoteStandby_stillInRecovery(t *testing.T) {
	t.Parallel()

	admin := &fakePromoteAdmin{recovery: "t"}
	err := PromoteStandby(context.Background(), admin)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "still in recovery") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type fakePromoteAdmin struct {
	rows      string
	recovery  string
	lastQuery string
	queries   []string
}

func (f *fakePromoteAdmin) execSQL(_ context.Context, query string) (string, error) {
	f.lastQuery = query
	f.queries = append(f.queries, query)
	switch {
	case query == sqlSelectStreamingStandbys:
		return f.rows, nil
	case query == sqlPromoteStandby:
		return "", nil
	case query == sqlCheckNotInRecovery:
		if f.recovery == "" {
			return "", errors.New("recovery not configured")
		}
		return f.recovery, nil
	default:
		return "", errors.New("unexpected query: " + query)
	}
}
