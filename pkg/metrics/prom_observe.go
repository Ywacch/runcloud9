package metrics

import (
	"context"
	"fmt"
	"math"
)

type collectContextKey struct{}

type promMetaKey struct{}

type promMeta struct {
	template string
	caller   string
	path     string
}

// PromQueryObserver is invoked once per Prometheus HTTP instant query in executePromInstant.
type PromQueryObserver func(path, caller, template string)

// CollectObserver is invoked once per session publisher Collect (or test hook), success or error.
type CollectObserver func(template string, seconds float64)

// FrameBytesObserver is invoked once per metrics WebSocket frame written.
type FrameBytesObserver func(n int)

// MetricsQueryObserver is invoked after a metrics query batch (source, count).
type MetricsQueryObserver func(source string, count int)

func withCollectNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, collectContextKey{}, namespace)
}

func collectNamespaceFrom(ctx context.Context) string {
	ns, _ := ctx.Value(collectContextKey{}).(string)
	return ns
}

// WithPromMeta attaches Prometheus query counter labels to ctx for executePromInstant.
func WithPromMeta(ctx context.Context, template, caller, path string) context.Context {
	return context.WithValue(ctx, promMetaKey{}, promMeta{
		template: template,
		caller:   caller,
		path:     path,
	})
}

func promMetaFrom(ctx context.Context) promMeta {
	m, _ := ctx.Value(promMetaKey{}).(promMeta)
	return m
}

func withPromCaller(ctx context.Context, caller string) context.Context {
	m := promMetaFrom(ctx)
	return WithPromMeta(ctx, m.template, caller, m.path)
}

func (c *MetricsCollector) SetMetricsQueryObserver(fn MetricsQueryObserver) {
	if c == nil {
		return
	}
	c.metricsQueryObserver = fn
}

func (c *MetricsCollector) SetPromQueryObserver(fn PromQueryObserver) {
	if c == nil {
		return
	}
	c.promQueryObserver = fn
}

func (c *MetricsCollector) SetCollectObserver(fn CollectObserver) {
	if c == nil {
		return
	}
	c.collectObserver = fn
}

func (c *MetricsCollector) SetFrameBytesObserver(fn FrameBytesObserver) {
	if c == nil {
		return
	}
	c.frameBytesObserver = fn
}

func (c *MetricsCollector) observeFrameBytes(n int) {
	if c == nil || c.frameBytesObserver == nil {
		return
	}
	c.frameBytesObserver(n)
}

func parsePromValue(v any) float64 {
	valStr, ok := v.(string)
	if !ok {
		return 0
	}
	var val float64
	_, _ = fmt.Sscanf(valStr, "%f", &val)
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return 0
	}
	return val
}
