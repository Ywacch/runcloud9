package chaos

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildPartitionNetworkPolicy(t *testing.T) {
	expires := time.Date(2026, 9, 12, 15, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		input          PartitionPolicyInput
		wantPolicyName string
		wantDestApp    string
		wantPort       int32
		wantExcludeKey string
		wantExcludeVal string
		wantExcludeOp  metav1.LabelSelectorOperator
		wantObserverNS []string
		wantErrSubstr  string
	}{
		{
			name: "split_primary_replica blocks replica app on primary",
			input: PartitionPolicyInput{
				Namespace:       "cloud9-test",
				Scenario:        "split_primary_replica",
				TargetComponent: "db",
				ExpiresAt:       expires,
				DestAppLabel:    "postgres",
				Port:            postgresPartitionPort,
				Exclude:         PartitionPeerExclusion{AppLabel: "postgres-replica"},
			},
			wantPolicyName: "c9-chaos-split_primary_replica",
			wantDestApp:    "postgres",
			wantPort:       postgresPartitionPort,
			wantExcludeKey: "app",
			wantExcludeVal: "postgres-replica",
			wantExcludeOp:  metav1.LabelSelectorOpNotIn,
			wantObserverNS: []string{observerNamespaceCloud9System, observerNamespaceMonitoring},
		},
		{
			name: "stall_replica blocks one replica pod on primary",
			input: PartitionPolicyInput{
				Namespace:       "cloud9-test",
				Scenario:        "stall_replica",
				TargetComponent: "db",
				ExpiresAt:       expires,
				DestAppLabel:    "postgres",
				Port:            postgresPartitionPort,
				Exclude:         PartitionPeerExclusion{PodName: "postgres-replica-0"},
			},
			wantPolicyName: "c9-chaos-stall_replica",
			wantDestApp:    "postgres",
			wantPort:       postgresPartitionPort,
			wantExcludeKey: "statefulset.kubernetes.io/pod-name",
			wantExcludeVal: "postgres-replica-0",
			wantExcludeOp:  metav1.LabelSelectorOpNotIn,
			wantObserverNS: []string{observerNamespaceCloud9System, observerNamespaceMonitoring},
		},
		{
			name: "split_app_store blocks app-server on redis",
			input: PartitionPolicyInput{
				Namespace:       "cloud9-test",
				Scenario:        "split_app_store",
				TargetComponent: "cache",
				ExpiresAt:       expires,
				DestAppLabel:    "redis",
				Port:            redisPartitionPort,
				Exclude:         PartitionPeerExclusion{AppLabel: "app-server"},
			},
			wantPolicyName: "c9-chaos-split_app_store",
			wantDestApp:    "redis",
			wantPort:       redisPartitionPort,
			wantExcludeKey: "app",
			wantExcludeVal: "app-server",
			wantExcludeOp:  metav1.LabelSelectorOpNotIn,
			wantObserverNS: []string{observerNamespaceCloud9System, observerNamespaceMonitoring},
		},
		{
			name: "invalid port",
			input: PartitionPolicyInput{
				Namespace:       "cloud9-test",
				Scenario:        "split_app_store",
				TargetComponent: "cache",
				ExpiresAt:       expires,
				DestAppLabel:    "redis",
				Port:            8080,
				Exclude:         PartitionPeerExclusion{AppLabel: "app-server"},
			},
			wantErrSubstr: "partition port must be",
		},
		{
			name: "both exclusion fields set",
			input: PartitionPolicyInput{
				Namespace:       "cloud9-test",
				Scenario:        "stall_replica",
				TargetComponent: "db",
				ExpiresAt:       expires,
				DestAppLabel:    "postgres",
				Port:            postgresPartitionPort,
				Exclude:         PartitionPeerExclusion{AppLabel: "postgres-replica", PodName: "postgres-replica-0"},
			},
			wantErrSubstr: "exactly one of AppLabel or PodName",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policy, err := BuildPartitionNetworkPolicy(tc.input)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("BuildPartitionNetworkPolicy: %v", err)
			}
			assertPartitionPolicy(t, policy, tc.input, tc.wantPolicyName, tc.wantDestApp, tc.wantPort, tc.wantExcludeKey, tc.wantExcludeVal, tc.wantExcludeOp, tc.wantObserverNS)
		})
	}
}

func assertPartitionPolicy(
	t *testing.T,
	policy *networkingv1.NetworkPolicy,
	input PartitionPolicyInput,
	wantName string,
	wantDestApp string,
	wantPort int32,
	wantExcludeKey string,
	wantExcludeVal string,
	wantExcludeOp metav1.LabelSelectorOperator,
	wantObserverNS []string,
) {
	t.Helper()
	if policy.Name != wantName {
		t.Fatalf("name: got %q want %q", policy.Name, wantName)
	}
	if policy.Namespace != input.Namespace {
		t.Fatalf("namespace: got %q want %q", policy.Namespace, input.Namespace)
	}
	if policy.Labels[ChaosLabelScenario] != input.Scenario {
		t.Fatalf("scenario label: got %q want %q", policy.Labels[ChaosLabelScenario], input.Scenario)
	}
	if policy.Labels[ChaosLabelTarget] != input.TargetComponent {
		t.Fatalf("target label: got %q want %q", policy.Labels[ChaosLabelTarget], input.TargetComponent)
	}
	if policy.Annotations[ChaosAnnotationExpiresAt] != input.ExpiresAt.UTC().Format(time.RFC3339) {
		t.Fatalf("expires annotation: got %q want %q", policy.Annotations[ChaosAnnotationExpiresAt], input.ExpiresAt.UTC().Format(time.RFC3339))
	}

	spec := policy.Spec
	if got := spec.PodSelector.MatchLabels["app"]; got != wantDestApp {
		t.Fatalf("dest app selector: got %q want %q", got, wantDestApp)
	}
	if len(spec.PolicyTypes) != 1 || spec.PolicyTypes[0] != networkingv1.PolicyTypeIngress {
		t.Fatalf("policy types: got %v want [Ingress]", spec.PolicyTypes)
	}
	if len(spec.Ingress) != 2 {
		t.Fatalf("ingress rules: got %d want 2", len(spec.Ingress))
	}

	partitionRule := spec.Ingress[0]
	if len(partitionRule.From) != 1 {
		t.Fatalf("partition from peers: got %d want 1", len(partitionRule.From))
	}
	assertPeerExclusion(t, partitionRule.From[0], wantExcludeKey, wantExcludeVal, wantExcludeOp)
	if len(partitionRule.Ports) != 1 {
		t.Fatalf("partition ports: got %d want 1", len(partitionRule.Ports))
	}
	if partitionRule.Ports[0].Port == nil || partitionRule.Ports[0].Port.IntVal != wantPort {
		t.Fatalf("partition port: got %v want %d", partitionRule.Ports[0].Port, wantPort)
	}
	if partitionRule.Ports[0].Protocol == nil || *partitionRule.Ports[0].Protocol != corev1.ProtocolTCP {
		t.Fatalf("partition protocol: got %v want TCP", partitionRule.Ports[0].Protocol)
	}

	observerRule := spec.Ingress[1]
	if len(observerRule.From) != len(wantObserverNS) {
		t.Fatalf("observer from peers: got %d want %d", len(observerRule.From), len(wantObserverNS))
	}
	if len(observerRule.Ports) != 0 {
		t.Fatalf("observer rule should not restrict ports")
	}
	for i, wantNS := range wantObserverNS {
		peer := observerRule.From[i]
		if peer.NamespaceSelector == nil {
			t.Fatalf("observer peer %d missing namespace selector", i)
		}
		got := peer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"]
		if got != wantNS {
			t.Fatalf("observer namespace %d: got %q want %q", i, got, wantNS)
		}
	}
}

func assertPeerExclusion(
	t *testing.T,
	peer networkingv1.NetworkPolicyPeer,
	wantKey string,
	wantVal string,
	wantOp metav1.LabelSelectorOperator,
) {
	t.Helper()
	if peer.PodSelector == nil {
		t.Fatal("expected pod selector on partition peer")
	}
	exprs := peer.PodSelector.MatchExpressions
	if len(exprs) != 1 {
		t.Fatalf("match expressions: got %d want 1", len(exprs))
	}
	expr := exprs[0]
	if expr.Key != wantKey {
		t.Fatalf("exclude key: got %q want %q", expr.Key, wantKey)
	}
	if expr.Operator != wantOp {
		t.Fatalf("exclude operator: got %q want %q", expr.Operator, wantOp)
	}
	if len(expr.Values) != 1 || expr.Values[0] != wantVal {
		t.Fatalf("exclude values: got %v want [%q]", expr.Values, wantVal)
	}
}
