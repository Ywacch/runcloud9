package kubeobs

import (
	"context"
	"net/url"
	"time"

	clientmetrics "k8s.io/client-go/tools/metrics"
)

// ThrottleLatency is the client-go rate-limiter latency hook.
// client-go calls Observe for every Wait, including a wait of zero.
type ThrottleLatency struct {
	Obs RequestObserver
}

// Observe classifies the request and records the wait on Obs.
func (t ThrottleLatency) Observe(ctx context.Context, verb string, u url.URL, latency time.Duration) {
	if t.Obs == nil || latency < 0 {
		return
	}
	gotVerb, resource := Classify(verb, u.Path)
	t.Obs.ObserveThrottleWait(gotVerb, resource, normalizeCaller(CallerFrom(ctx)), latency.Seconds())
}

// RegisterThrottleLatency installs the process-wide client-go rate-limiter hook.
// client-go accepts this registration once per process. Later calls do nothing.
func RegisterThrottleLatency(obs RequestObserver) {
	if obs == nil {
		return
	}
	clientmetrics.Register(clientmetrics.RegisterOpts{
		RateLimiterLatency: ThrottleLatency{Obs: obs},
	})
}
