package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Config controls namespace reaping.
type Config struct {
	Now   func() time.Time
	Grace time.Duration
}

var protectedNamespaces = map[string]struct{}{
	"cloud9-system":   {},
	"default":         {},
	"kube-node-lease": {},
	"kube-public":     {},
	"kube-system":     {},
	"monitoring":      {},
}

// Reap deletes namespaces whose expiry label is before Now minus Grace.
// Namespaces without the label or in protectedNamespaces are left alone.
func Reap(ctx context.Context, client kubernetes.Interface, cfg Config) (deleted []string, err error) {
	if client == nil {
		return nil, fmt.Errorf("reaper: kubernetes client is required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Grace <= 0 {
		cfg.Grace = 5 * time.Minute
	}
	list, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("reaper: list namespaces: %w", err)
	}
	now := cfg.Now().UTC()
	for i := range list.Items {
		ns := &list.Items[i]
		if _, skip := protectedNamespaces[ns.Name]; skip {
			continue
		}
		raw, ok := ns.Labels[deployer.NamespaceExpiresAtLabel]
		if !ok {
			continue
		}
		unixSec, perr := strconv.ParseInt(raw, 10, 64)
		if perr != nil {
			continue
		}
		expiresAt := time.Unix(unixSec, 0).UTC()
		if now.Before(expiresAt.Add(cfg.Grace)) {
			continue
		}
		if derr := client.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{}); derr != nil {
			return deleted, fmt.Errorf("reaper: delete namespace %s: %w", ns.Name, derr)
		}
		deleted = append(deleted, ns.Name)
	}
	return deleted, nil
}
