package metrics

import (
	"context"
	"log"
	"strings"
	"time"

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

func (c *MetricsCollector) sigCompat(ctx context.Context, ns string, id signals.ID, oldPromQL string) (float64, bool) {
	v, ok := c.readSignalCompat(ctx, ns, id, oldPromQL)
	if signalCompareEnabled() && strings.TrimSpace(oldPromQL) != "" {
		sigVal, sigOK := c.signalForCompare(ctx, ns, id)
		c.compareSignalScalar(ctx, ns, id, sigVal, sigOK, oldPromQL)
	}
	return v, ok
}

// sigCompatByApp reads one app label from a PerApp signal. When compare mode is on, oldPromQL
// is still evaluated against the returned value (for example a pod-regex instant query).
func (c *MetricsCollector) sigCompatByApp(ctx context.Context, ns string, id signals.ID, app, oldPromQL string) (float64, bool) {
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
	out, okOut := applySignalMissing(v, present, sig.Missing)
	if signalCompareEnabled() && strings.TrimSpace(oldPromQL) != "" {
		var sigVal float64
		var sigOK bool
		if m != nil {
			sigVal, sigOK = m[app]
		}
		c.compareSignalScalar(ctx, ns, id, sigVal, sigOK, oldPromQL)
	}
	return out, okOut
}

func (c *MetricsCollector) signalForCompare(ctx context.Context, ns string, id signals.ID) (float64, bool) {
	sig, ok := signals.Lookup(id)
	if !ok || ns == "" {
		return 0, false
	}
	v, present := c.readSignalScalar(ctx, ns, sig)
	return applySignalMissing(v, present, sig.Missing)
}

func (c *MetricsCollector) readSignalCompat(ctx context.Context, ns string, id signals.ID, legacyPromQL string) (float64, bool) {
	sig, ok := signals.Lookup(id)
	if !ok || ns == "" {
		return 0, false
	}
	if h := c.MetricsHub(); h != nil {
		v, present := c.readSignalScalar(ctx, ns, sig)
		return applySignalMissing(v, present, sig.Missing)
	}
	legacyPromQL = strings.TrimSpace(legacyPromQL)
	if legacyPromQL != "" {
		if v, present, hit := c.lookupClusterSampleScalar(ctx, legacyPromQL); hit {
			return applySignalMissing(v, present, sig.Missing)
		}
	}
	v, present := c.directSignalScalarWithLegacy(ctx, ns, sig, legacyPromQL)
	return applySignalMissing(v, present, sig.Missing)
}

func (c *MetricsCollector) lookupClusterSampleScalar(ctx context.Context, promql string) (float64, bool, bool) {
	if c == nil || !c.clusterSampleEnabled || promql == "" {
		return 0, false, false
	}
	sample := c.ensureClusterSample(ctx)
	if sample == nil {
		return 0, false, false
	}
	if time.Since(sample.fetchedAt) >= clusterSampleTick {
		return 0, false, false
	}
	v, present, ok := c.lookupSampleScalar(sample, ctx, promql)
	if !ok {
		return 0, false, false
	}
	return v, present, true
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
	return c.directSignalScalarWithLegacy(ctx, ns, sig, "")
}

func (c *MetricsCollector) directSignalScalarWithLegacy(ctx context.Context, ns string, sig signals.Signal, legacyPromQL string) (float64, bool) {
	promql, err := signals.RenderNamespace(sig, ns)
	if err != nil {
		return 0, false
	}
	ctx = withSignalDirectProm(ctx)
	// Prefer the call site's PromQL for histogram_quantile. The catalog form groups by
	// namespace and app; live frames and cluster-sample keys still use by (le).
	if strings.Contains(promql, "histogram_quantile(") || strings.Contains(legacyPromQL, "histogram_quantile(") {
		if legacyPromQL != "" {
			v, present, err := c.queryScalar(ctx, legacyPromQL)
			if err == nil && present {
				return v, true
			}
		}
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
		if legacyPromQL != "" {
			v, present, err := c.queryScalar(ctx, legacyPromQL)
			if err == nil && present {
				return v, true
			}
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

func (c *MetricsCollector) compareSignalScalar(ctx context.Context, ns string, id signals.ID, signalVal float64, signalOK bool, oldPromQL string) {
	oldVal, oldOK, err := c.queryScalarDirect(ctx, oldPromQL)
	if err != nil {
		oldOK = false
	}
	match := promValuesMatch(signalVal, oldVal, signalOK, oldOK)
	result := "match"
	if !match {
		result = "mismatch"
		log.Printf("metrics: signal compare mismatch ns=%s signal=%s hub=%v old=%v ok=(%v,%v)", ns, id, signalVal, oldVal, signalOK, oldOK)
	}
	if c != nil && c.signalCompareObserver != nil {
		c.signalCompareObserver(string(id), result)
	}
}
