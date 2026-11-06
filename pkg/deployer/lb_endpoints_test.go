package deployer

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestHostsFromEndpoints_ReadyAddresses(t *testing.T) {
	t.Parallel()
	ep := &corev1.Endpoints{
		Subsets: []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{
					{IP: "", TargetRef: &corev1.ObjectReference{Name: "skip"}},
					{IP: "10.1.1.1", TargetRef: &corev1.ObjectReference{Name: "pod-a"}},
				},
			},
			{
				Addresses: []corev1.EndpointAddress{
					{IP: "10.1.1.2", TargetRef: &corev1.ObjectReference{Name: "pod-b"}},
				},
			},
		},
	}
	hosts, snaps := hostsFromEndpoints(ep)
	if len(hosts) != 2 || hosts[0] != "10.1.1.1" || hosts[1] != "10.1.1.2" {
		t.Fatalf("hosts = %v", hosts)
	}
	if len(snaps) != 2 || snaps[0].PodName != "pod-a" || snaps[1].PodName != "pod-b" {
		t.Fatalf("snaps = %+v", snaps)
	}
}

func TestHostsFromEndpointSlices_SkipsNotReady(t *testing.T) {
	t.Parallel()
	ready := true
	notReady := false
	slices := []*discoveryv1.EndpointSlice{
		{
			ObjectMeta:  metav1.ObjectMeta{Name: "slice-1", Namespace: "ns", Labels: map[string]string{discoveryv1.LabelServiceName: "app-server"}},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses:  []string{"10.0.0.1"},
					TargetRef:  &corev1.ObjectReference{Name: "p1"},
					Conditions: discoveryv1.EndpointConditions{Ready: &ready},
				},
				{
					Addresses:  []string{"10.0.0.2"},
					TargetRef:  &corev1.ObjectReference{Name: "p2"},
					Conditions: discoveryv1.EndpointConditions{Ready: &notReady},
				},
			},
		},
	}
	hosts, snaps := hostsFromEndpointSlices(slices)
	if len(hosts) != 1 || hosts[0] != "10.0.0.1" {
		t.Fatalf("hosts = %v want [10.0.0.1]", hosts)
	}
	if len(snaps) != 1 || snaps[0].PodIP != "10.0.0.1" {
		t.Fatalf("snaps = %+v", snaps)
	}
}

type fakeEndpointView struct {
	ep     *corev1.Endpoints
	slices []*discoveryv1.EndpointSlice
	pods   []*corev1.Pod
}

func (f *fakeEndpointView) Endpoints(namespace, name string) (*corev1.Endpoints, error) {
	if f == nil {
		return nil, nil
	}
	if f.ep != nil && f.ep.Namespace == namespace && f.ep.Name == name {
		return f.ep, nil
	}
	return nil, nil
}

func (f *fakeEndpointView) EndpointSlices(namespace, serviceName string) ([]*discoveryv1.EndpointSlice, error) {
	if f == nil {
		return nil, nil
	}
	return f.slices, nil
}

func (f *fakeEndpointView) Pods(namespace string) ([]*corev1.Pod, error) {
	if f == nil {
		return nil, nil
	}
	return f.pods, nil
}

func TestResolveUpstream_ViewDoesNotGet(t *testing.T) {
	t.Parallel()
	ns := "ns-view"
	view := &fakeEndpointView{
		ep: &corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
			Subsets: []corev1.EndpointSubset{{
				Addresses: []corev1.EndpointAddress{
					{IP: "10.5.0.1", TargetRef: &corev1.ObjectReference{Name: "a"}},
				},
			}},
		},
	}
	// No Endpoints object in the clientset: a mistaken API Get would fail and return service fallback.
	client := fake.NewSimpleClientset()
	d := NewK8sDeployer(client, "lb")
	d.SetEndpointView(view)
	hosts, snaps := d.resolveAppServerUpstream(context.Background(), ns)
	if len(hosts) != 1 || hosts[0] != "10.5.0.1" {
		t.Fatalf("hosts = %v", hosts)
	}
	if len(snaps) != 1 || snaps[0].PodIP != "10.5.0.1" {
		t.Fatalf("snaps = %+v", snaps)
	}
}

func TestNginxWeights_ViewPods(t *testing.T) {
	t.Parallel()
	ns := "ns-weights"
	view := &fakeEndpointView{
		pods: []*corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "app-1",
					Namespace: ns,
					Labels: map[string]string{
						"app":                "app-server",
						"deployment":         ns,
						instanceSizeLabelKey: "small",
					},
				},
				Status: corev1.PodStatus{PodIP: "10.0.0.5"},
			},
		},
	}
	client := fake.NewSimpleClientset()
	got := nginxWeightsForHosts(context.Background(), client, ns, []string{"10.0.0.5"}, view)
	want := nginxWeightForSize("small")
	if len(got) != 1 || got[0] != want {
		t.Fatalf("weights = %v want [%d]", got, want)
	}
}
