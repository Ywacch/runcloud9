package kubeobs

import "context"

type callerKey struct{}

// WithCaller attaches a logical caller name to ctx for Kubernetes client metrics.
func WithCaller(ctx context.Context, caller string) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

// CallerFrom returns the caller set by WithCaller, or empty when unset.
func CallerFrom(ctx context.Context) string {
	c, _ := ctx.Value(callerKey{}).(string)
	return c
}

// normalizeCaller maps ctx callers to allowed label values.
func normalizeCaller(caller string) string {
	switch caller {
	case "clusterstate", "deployer", "metrics", "lb", "teardown":
		return caller
	default:
		return "unknown"
	}
}
