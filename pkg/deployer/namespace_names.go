package deployer

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// NamespaceNameSet lists every namespace name in the cluster (one API call).
func NamespaceNameSet(ctx context.Context, client kubernetes.Interface) (map[string]struct{}, error) {
	if client == nil {
		return nil, fmt.Errorf("deployer.NamespaceNameSet: kubernetes client is required")
	}
	list, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("deployer.NamespaceNameSet: list: %w", err)
	}
	names := make(map[string]struct{}, len(list.Items))
	for _, item := range list.Items {
		names[item.Name] = struct{}{}
	}
	return names, nil
}
