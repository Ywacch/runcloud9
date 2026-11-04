package api

import (
	"sync"

	"github.com/ywacch/cloud9/pkg/kubeobs"
)

var (
	sharedCPMetricsMu sync.Mutex
	sharedCPMetrics   *controlPlaneMetrics
)

// BootstrapControlPlaneMetrics allocates control-plane Prometheus collectors for process
// lifetime and returns the Kubernetes client observer. Call once from cmd/api before
// building the Kubernetes clientset so transport wrapping shares the same registry as /metrics.
func BootstrapControlPlaneMetrics() kubeobs.RequestObserver {
	sharedCPMetricsMu.Lock()
	defer sharedCPMetricsMu.Unlock()
	if sharedCPMetrics == nil {
		sharedCPMetrics = newControlPlaneMetrics()
	}
	return sharedCPMetrics
}

func controlPlaneMetricsForHandler() *controlPlaneMetrics {
	sharedCPMetricsMu.Lock()
	defer sharedCPMetricsMu.Unlock()
	if sharedCPMetrics != nil {
		return sharedCPMetrics
	}
	return newControlPlaneMetrics()
}
