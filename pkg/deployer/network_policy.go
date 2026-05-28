package deployer

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	defaultDenyEgressPolicyName      = "default-deny-egress"
	allowDNSAndNamespaceEgressPolicy = "allow-dns-and-namespace-egress"
)

// ensureNamespaceNetworkPolicies applies a baseline egress posture in each deployment namespace:
// 1) deny all egress by default, 2) allow only DNS and same-namespace traffic.
// This keeps component-to-component traffic working while blocking arbitrary outbound internet access.
func (k *K8sDeployer) ensureNamespaceNetworkPolicies(ctx context.Context, namespace string) error {
	policies := []*networkingv1.NetworkPolicy{
		defaultDenyEgressPolicy(namespace),
		allowDNSAndNamespaceEgress(namespace),
	}
	for _, p := range policies {
		if _, err := k.clientset.NetworkingV1().NetworkPolicies(namespace).Create(ctx, p, metav1.CreateOptions{}); err != nil {
			if errors.IsAlreadyExists(err) {
				continue
			}
			return fmt.Errorf("create networkpolicy %q: %w", p.Name, err)
		}
	}
	return nil
}

func defaultDenyEgressPolicy(namespace string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultDenyEgressPolicyName,
			Namespace: namespace,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
		},
	}
}

// allowDNSAndNamespaceEgress permits:
// - DNS lookups via kube-dns in kube-system on TCP/UDP 53
// - DNS to node-local cache IP (GKE NodeLocal DNSCache: 169.254.20.10)
// - traffic to pods in the same deployment namespace
// Everything else remains denied by default-deny-egress.
func allowDNSAndNamespaceEgress(namespace string) *networkingv1.NetworkPolicy {
	udp53 := intstr.FromInt(53)
	tcp53 := intstr.FromInt(53)
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      allowDNSAndNamespaceEgressPolicy,
			Namespace: namespace,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					To: []networkingv1.NetworkPolicyPeer{
						{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"kubernetes.io/metadata.name": "kube-system"},
							},
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"k8s-app": "kube-dns"},
							},
						},
						{
							IPBlock: &networkingv1.IPBlock{
								CIDR: "169.254.20.10/32",
							},
						},
					},
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: protocolPtr(corev1.ProtocolUDP), Port: &udp53},
						{Protocol: protocolPtr(corev1.ProtocolTCP), Port: &tcp53},
					},
				},
				{
					To: []networkingv1.NetworkPolicyPeer{
						{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"kubernetes.io/metadata.name": namespace},
							},
						},
					},
				},
			},
		},
	}
}

func protocolPtr(p corev1.Protocol) *corev1.Protocol {
	return &p
}
