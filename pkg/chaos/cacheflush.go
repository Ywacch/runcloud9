package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/client-go/kubernetes"
)

// CacheFlushScenario flushes the deployment cache via the engine-specific admin command.
type CacheFlushScenario struct {
	Client kubernetes.Interface
}

// Apply execs the cache flush command against the resolved cache pod.
func (s *CacheFlushScenario) Apply(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error {
	if targetComponent != "cache" {
		return fmt.Errorf("cache_flush only supports targetComponent \"cache\", got %q", targetComponent)
	}

	topo := deployer.StorytellingTopologyFromDesign(&design, design.Template)
	cmd, err := cacheFlushCommand(topo.CacheEngine)
	if err != nil {
		return err
	}

	resolved, err := resolveTarget(targetComponent, design)
	if err != nil {
		return err
	}

	podName, err := findRunningPod(ctx, s.Client, namespace, resolved.AppLabel)
	if err != nil {
		return fmt.Errorf("no running cache pod: %w", err)
	}

	stdout, stderr, err := execInPod(ctx, s.Client, namespace, podName, resolved.Container, cmd)
	if err != nil {
		return err
	}

	out := strings.TrimSpace(stdout)
	if out == "" {
		out = strings.TrimSpace(stderr)
	}
	slog.Info("chaos cache flush", "namespace", namespace, "pod", podName, "app", resolved.AppLabel, "result", out)
	return nil
}

// Revert is a no-op for cache_flush.
func (s *CacheFlushScenario) Revert(context.Context, string, deployer.DeploymentDesign, string) error {
	return nil
}

// Stateful reports that cache_flush does not leave reversible cluster state.
func (s *CacheFlushScenario) Stateful() bool { return false }

// Description returns UI copy for the scenario.
func (s *CacheFlushScenario) Description(targetComponent string) string {
	_ = targetComponent
	return "Flush cache — triggers cache miss storm on DB"
}
