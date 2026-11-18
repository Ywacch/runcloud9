package deployer

import corev1 "k8s.io/api/core/v1"

// Session and balloon PriorityClass names must match k8s/base/cloud9-balloon.yaml.
const (
	SessionPriorityClassName = "cloud9-session"
	BalloonPriorityClassName = "cloud9-balloon"
)

func applySessionPodPriority(spec *corev1.PodSpec) {
	if spec == nil {
		return
	}
	spec.PriorityClassName = SessionPriorityClassName
}
