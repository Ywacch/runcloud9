package chaos

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// ChaosLabelScenario labels a partition NetworkPolicy with the scenario id.
	ChaosLabelScenario = "cloud9.chaos/scenario"
	// ChaosLabelTarget labels a partition NetworkPolicy with the canvas target component.
	ChaosLabelTarget = "cloud9.chaos/target"
	// ChaosAnnotationExpiresAt holds the RFC3339 time when the partition should auto-revert.
	ChaosAnnotationExpiresAt = "cloud9.chaos/expires-at"
	// ChaosAnnotationReplicaPod names the stalled postgres replica for Heal restore.
	ChaosAnnotationReplicaPod = "cloud9.chaos/replica-pod"
	chaosPolicyNamePrefix     = "c9-chaos-"

	postgresPartitionPort = int32(5432)
	redisPartitionPort    = int32(6379)

	observerNamespaceCloud9System = "cloud9-system"
	observerNamespaceMonitoring   = "monitoring"
)

// PartitionPeerExclusion selects which source pods are blocked from reaching the destination.
// Set AppLabel or PodName, not both.
type PartitionPeerExclusion struct {
	// AppLabel blocks pods with app=<value> in the deployment namespace.
	AppLabel string
	// PodName blocks the pod with statefulset.kubernetes.io/pod-name=<value>.
	PodName string
}

// PartitionPolicyInput configures an ingress partition NetworkPolicy on destination pods.
type PartitionPolicyInput struct {
	Namespace       string
	Scenario        string
	TargetComponent string
	ExpiresAt       time.Time
	DestAppLabel    string
	Port            int32
	Exclude         PartitionPeerExclusion
}

// BuildPartitionNetworkPolicy returns an ingress policy that blocks the excluded peer while
// keeping observer and monitoring traffic allowed. Policies union with the baseline egress
// posture; partitions must be ingress rules on the destination, not egress denies.
func BuildPartitionNetworkPolicy(in PartitionPolicyInput) (*networkingv1.NetworkPolicy, error) {
	if in.Namespace == "" {
		return nil, fmt.Errorf("chaos: partition namespace is required")
	}
	if in.Scenario == "" {
		return nil, fmt.Errorf("chaos: partition scenario is required")
	}
	if in.TargetComponent == "" {
		return nil, fmt.Errorf("chaos: partition targetComponent is required")
	}
	if in.DestAppLabel == "" {
		return nil, fmt.Errorf("chaos: partition destAppLabel is required")
	}
	if in.Port != postgresPartitionPort && in.Port != redisPartitionPort {
		return nil, fmt.Errorf("chaos: partition port must be %d or %d", postgresPartitionPort, redisPartitionPort)
	}
	if err := validatePeerExclusion(in.Exclude); err != nil {
		return nil, err
	}
	if in.ExpiresAt.IsZero() {
		return nil, fmt.Errorf("chaos: partition expiresAt is required")
	}

	port := intstr.FromInt32(in.Port)
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      partitionPolicyName(in.Scenario),
			Namespace: in.Namespace,
			Labels: map[string]string{
				ChaosLabelScenario: in.Scenario,
				ChaosLabelTarget:   in.TargetComponent,
			},
			Annotations: map[string]string{
				ChaosAnnotationExpiresAt: in.ExpiresAt.UTC().Format(time.RFC3339),
			},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{"app": in.DestAppLabel},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From:  []networkingv1.NetworkPolicyPeer{sameNamespacePeerAllowingExclusion(in.Exclude)},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: protocolTCP(), Port: &port}},
				},
				{
					From: observerNamespacePeers(),
				},
			},
		},
	}, nil
}

func validatePeerExclusion(ex PartitionPeerExclusion) error {
	hasApp := ex.AppLabel != ""
	hasPod := ex.PodName != ""
	if hasApp == hasPod {
		return fmt.Errorf("chaos: partition exclusion requires exactly one of AppLabel or PodName")
	}
	return nil
}

func sameNamespacePeerAllowingExclusion(ex PartitionPeerExclusion) networkingv1.NetworkPolicyPeer {
	if ex.PodName != "" {
		return networkingv1.NetworkPolicyPeer{
			PodSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "statefulset.kubernetes.io/pod-name",
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{ex.PodName},
				}},
			},
		}
	}
	return networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "app",
				Operator: metav1.LabelSelectorOpNotIn,
				Values:   []string{ex.AppLabel},
			}},
		},
	}
}

func observerNamespacePeers() []networkingv1.NetworkPolicyPeer {
	return []networkingv1.NetworkPolicyPeer{
		namespacePeer(observerNamespaceCloud9System),
		namespacePeer(observerNamespaceMonitoring),
	}
}

func namespacePeer(name string) networkingv1.NetworkPolicyPeer {
	return networkingv1.NetworkPolicyPeer{
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"kubernetes.io/metadata.name": name},
		},
	}
}

func protocolTCP() *corev1.Protocol {
	p := corev1.ProtocolTCP
	return &p
}
