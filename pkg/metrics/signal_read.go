package metrics

import (
	"context"
	"strings"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

func (c *MetricsCollector) sig(ctx context.Context, ns string, id signals.ID) (float64, bool) {
	sig, ok := signals.Lookup(id)
	if !ok || ns == "" {
		return 0, false
	}
	v, present := c.readSignalScalar(ctx, ns, sig)
	return applySignalMissing(v, present, sig.Missing)
}

// sigForApp reads one app label from a PerApp signal.
func (c *MetricsCollector) sigForApp(ctx context.Context, ns string, id signals.ID, app string) (float64, bool) {
	sig, ok := signals.Lookup(id)
	if !ok || ns == "" || app == "" {
		return 0, false
	}
	m := c.sigByApp(ctx, ns, id)
	var v float64
	var present bool
	if m != nil {
		v, present = m[app]
	}
	return applySignalMissing(v, present, sig.Missing)
}

func (c *MetricsCollector) sigByApp(ctx context.Context, ns string, id signals.ID) map[string]float64 {
	sig, ok := signals.Lookup(id)
	if !ok || ns == "" || sig.Shape != signals.PerApp {
		return nil
	}
	if h := c.MetricsHub(); h != nil {
		snap := h.Snapshot()
		if snap == nil {
			return nil
		}
		m, present, _ := snap.ByApp(ns, id)
		if !present || len(m) == 0 {
			return nil
		}
		return m
	}
	promql, err := signals.RenderNamespace(sig, ns)
	if err != nil {
		return nil
	}
	ctx = withSignalDirectProm(ctx)
	return c.queryMultiResult(ctx, promql, "app")
}

func (c *MetricsCollector) sigByPod(ctx context.Context, ns string, id signals.ID) map[string]float64 {
	sig, ok := signals.Lookup(id)
	if !ok || ns == "" || sig.Shape != signals.PerPod {
		return nil
	}
	if h := c.MetricsHub(); h != nil {
		snap := h.Snapshot()
		if snap == nil {
			return nil
		}
		m, present, _ := snap.ByPod(ns, id)
		if !present || len(m) == 0 {
			return nil
		}
		return m
	}
	promql, err := signals.RenderNamespace(sig, ns)
	if err != nil {
		return nil
	}
	ctx = withSignalDirectProm(ctx)
	return c.queryMultiResult(ctx, promql, "pod")
}

func (c *MetricsCollector) readSignalScalar(ctx context.Context, ns string, sig signals.Signal) (float64, bool) {
	if h := c.MetricsHub(); h != nil {
		snap := h.Snapshot()
		if snap == nil {
			return 0, false
		}
		v, present, _ := snap.Scalar(ns, sig.ID, sig.Shape)
		return v, present
	}
	return c.directSignalScalar(ctx, ns, sig)
}

func (c *MetricsCollector) directSignalScalar(ctx context.Context, ns string, sig signals.Signal) (float64, bool) {
	promql, err := signals.RenderNamespace(sig, ns)
	if err != nil {
		return 0, false
	}
	ctx = withSignalDirectProm(ctx)
	if strings.Contains(promql, "histogram_quantile(") {
		v, present, err := c.queryScalar(ctx, promql)
		if err == nil && present {
			return v, true
		}
		return 0, false
	}
	switch sig.Shape {
	case signals.PerApp:
		m := c.queryMultiResult(ctx, promql, "app")
		if len(m) > 0 {
			var sum float64
			for _, v := range m {
				sum += v
			}
			return sum, true
		}
		v, present, err := c.queryScalar(ctx, promql)
		if err == nil && present {
			return v, true
		}
		return 0, false
	case signals.PerPod:
		m := c.queryMultiResult(ctx, promql, "pod")
		if len(m) > 0 {
			var sum float64
			for _, v := range m {
				sum += v
			}
			return sum, true
		}
		v, present, err := c.queryScalar(ctx, promql)
		if err == nil && present {
			return v, true
		}
		return 0, false
	default:
		v, present, err := c.queryScalar(ctx, promql)
		if err != nil {
			return 0, false
		}
		return v, present
	}
}

func withSignalDirectProm(ctx context.Context) context.Context {
	m := promMetaFrom(ctx)
	template := m.template
	if template == "" {
		template = "none"
	}
	return WithPromMeta(ctx, template, "signals", "direct")
}

func applySignalMissing(v float64, present bool, policy signals.Missing) (float64, bool) {
	if present {
		return v, true
	}
	if policy == signals.MissingIsZero {
		return 0, true
	}
	return 0, false
}
