package deployer

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodLister returns session pods for a namespace without implying an API list call.
type PodLister func(namespace string) ([]*corev1.Pod, error)

// SetPodLister installs a cache-backed pod source. Nil restores API list behavior.
func (k *K8sDeployer) SetPodLister(list PodLister) {
	if k == nil {
		return
	}
	k.podLister = list
}

type equalSelectorTerm struct {
	key   string
	value string
}

func parseEqualSelector(selector string) ([]equalSelectorTerm, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return nil, nil
	}
	parts := strings.Split(selector, ",")
	terms := make([]equalSelectorTerm, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("invalid pod label selector %q: empty term", selector)
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid pod label selector %q: want key=value terms", selector)
		}
		terms = append(terms, equalSelectorTerm{key: k, value: v})
	}
	return terms, nil
}

// matchEqualSelector reports whether labels satisfy every key=value term in selector.
// An empty selector matches nothing. A malformed selector returns an error.
func matchEqualSelector(labels map[string]string, selector string) (bool, error) {
	terms, err := parseEqualSelector(selector)
	if err != nil {
		return false, err
	}
	if len(terms) == 0 {
		return false, nil
	}
	for _, term := range terms {
		got, ok := labels[term.key]
		if !ok || got != term.value {
			return false, nil
		}
	}
	return true, nil
}

func podsMatchingEqualSelector(ptrs []*corev1.Pod, selector string) ([]corev1.Pod, error) {
	terms, err := parseEqualSelector(selector)
	if err != nil {
		return nil, err
	}
	if len(terms) == 0 {
		return nil, nil
	}
	out := make([]corev1.Pod, 0, len(ptrs))
	for _, p := range ptrs {
		if p == nil {
			continue
		}
		ok, err := matchEqualSelector(p.Labels, selector)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, *p.DeepCopy())
		}
	}
	return out, nil
}

func (k *K8sDeployer) listPodsForReadiness(ctx context.Context, namespace, labelSelector string) ([]corev1.Pod, error) {
	if k.podLister != nil {
		ptrs, err := k.podLister(namespace)
		if err != nil {
			return nil, err
		}
		return podsMatchingEqualSelector(ptrs, labelSelector)
	}
	list, err := k.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return nil, err
	}
	items := make([]corev1.Pod, len(list.Items))
	copy(items, list.Items)
	return items, nil
}
