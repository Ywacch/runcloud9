package clusterstate

import (
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func trimMeta(meta *metav1.ObjectMeta) {
	if meta == nil {
		return
	}
	meta.ManagedFields = nil
	meta.Annotations = nil
}

func trimContainerResources(c *corev1.Container) corev1.Container {
	if c == nil {
		return corev1.Container{}
	}
	return corev1.Container{
		Name:      c.Name,
		Resources: c.Resources,
	}
}

// trimPod returns a copy with only fields needed for readiness and future limit reads.
func trimPod(in *corev1.Pod) *corev1.Pod {
	if in == nil {
		return nil
	}
	out := in.DeepCopy()
	trimMeta(&out.ObjectMeta)
	out.Spec = corev1.PodSpec{
		Containers:     trimContainers(in.Spec.Containers),
		InitContainers: trimContainers(in.Spec.InitContainers),
	}
	return out
}

func trimContainers(in []corev1.Container) []corev1.Container {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.Container, len(in))
	for i := range in {
		out[i] = trimContainerResources(&in[i])
	}
	return out
}

func trimDeployment(in *appsv1.Deployment) *appsv1.Deployment {
	if in == nil {
		return nil
	}
	replicas := in.Spec.Replicas
	out := in.DeepCopy()
	trimMeta(&out.ObjectMeta)
	out.Spec = appsv1.DeploymentSpec{
		Replicas: replicas,
		Template: corev1.PodTemplateSpec{},
	}
	return out
}

func trimStatefulSet(in *appsv1.StatefulSet) *appsv1.StatefulSet {
	if in == nil {
		return nil
	}
	replicas := in.Spec.Replicas
	out := in.DeepCopy()
	trimMeta(&out.ObjectMeta)
	out.Spec = appsv1.StatefulSetSpec{
		Replicas: replicas,
		Template: corev1.PodTemplateSpec{},
	}
	return out
}

func trimHPA(in *autoscalingv2.HorizontalPodAutoscaler) *autoscalingv2.HorizontalPodAutoscaler {
	if in == nil {
		return nil
	}
	scaleRef := in.Spec.ScaleTargetRef
	desired := in.Status.DesiredReplicas
	current := in.Status.CurrentReplicas
	out := in.DeepCopy()
	trimMeta(&out.ObjectMeta)
	out.Spec = autoscalingv2.HorizontalPodAutoscalerSpec{
		ScaleTargetRef: scaleRef,
	}
	out.Status = autoscalingv2.HorizontalPodAutoscalerStatus{
		DesiredReplicas: desired,
		CurrentReplicas: current,
	}
	return out
}

func trimEndpoints(in *corev1.Endpoints) *corev1.Endpoints {
	if in == nil {
		return nil
	}
	out := in.DeepCopy()
	trimMeta(&out.ObjectMeta)
	subsets := make([]corev1.EndpointSubset, len(in.Subsets))
	for i, ss := range in.Subsets {
		subsets[i] = corev1.EndpointSubset{
			Addresses: ss.Addresses,
			Ports:     ss.Ports,
		}
	}
	out.Subsets = subsets
	return out
}

func trimEndpointSlice(in *discoveryv1.EndpointSlice) *discoveryv1.EndpointSlice {
	if in == nil {
		return nil
	}
	out := in.DeepCopy()
	trimMeta(&out.ObjectMeta)
	svcName := in.Labels[discoveryv1.LabelServiceName]
	if out.Labels == nil {
		out.Labels = map[string]string{}
	}
	out.Labels[discoveryv1.LabelServiceName] = svcName
	endpoints := make([]discoveryv1.Endpoint, len(in.Endpoints))
	for i, ep := range in.Endpoints {
		endpoints[i] = discoveryv1.Endpoint{
			Addresses: ep.Addresses,
			TargetRef: ep.TargetRef,
			Conditions: discoveryv1.EndpointConditions{
				Ready: ep.Conditions.Ready,
			},
		}
	}
	out.Endpoints = endpoints
	out.Ports = append([]discoveryv1.EndpointPort(nil), in.Ports...)
	out.AddressType = in.AddressType
	return out
}

func transformPod(obj any) (any, error) {
	if p, ok := obj.(*corev1.Pod); ok {
		return trimPod(p), nil
	}
	return obj, nil
}

func transformDeployment(obj any) (any, error) {
	if d, ok := obj.(*appsv1.Deployment); ok {
		return trimDeployment(d), nil
	}
	return obj, nil
}

func transformStatefulSet(obj any) (any, error) {
	if s, ok := obj.(*appsv1.StatefulSet); ok {
		return trimStatefulSet(s), nil
	}
	return obj, nil
}

func transformHPA(obj any) (any, error) {
	if h, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler); ok {
		return trimHPA(h), nil
	}
	return obj, nil
}

func transformEndpoints(obj any) (any, error) {
	if e, ok := obj.(*corev1.Endpoints); ok {
		return trimEndpoints(e), nil
	}
	return obj, nil
}

func transformEndpointSlice(obj any) (any, error) {
	if e, ok := obj.(*discoveryv1.EndpointSlice); ok {
		return trimEndpointSlice(e), nil
	}
	return obj, nil
}
