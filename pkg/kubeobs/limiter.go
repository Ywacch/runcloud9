package kubeobs

import (
	"context"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

const throttleReportMin = time.Millisecond

type countingLimiter struct {
	inner flowcontrol.RateLimiter
	obs   RequestObserver
}

// CountingLimiter wraps a client-go rate limiter and records throttle waits.
func CountingLimiter(inner flowcontrol.RateLimiter, obs RequestObserver) flowcontrol.RateLimiter {
	if inner == nil {
		return nil
	}
	return &countingLimiter{inner: inner, obs: obs}
}

func (l *countingLimiter) TryAccept() bool {
	return l.inner.TryAccept()
}

func (l *countingLimiter) Accept() {
	l.inner.Accept()
}

func (l *countingLimiter) Stop() {
	l.inner.Stop()
}

func (l *countingLimiter) QPS() float32 {
	return l.inner.QPS()
}

func (l *countingLimiter) Wait(ctx context.Context) error {
	start := time.Now()
	err := l.inner.Wait(ctx)
	l.reportWait(time.Since(start))
	return err
}

func (l *countingLimiter) reportWait(d time.Duration) {
	if l.obs == nil || d <= throttleReportMin {
		return
	}
	l.obs.ObserveThrottle(d.Seconds())
}

// FreshTokenBucket returns a new token bucket. Pass a non-nil observer to
// count waits longer than 1ms. A non-positive QPS or burst returns nil.
func FreshTokenBucket(qps float32, burst int, obs RequestObserver) flowcontrol.RateLimiter {
	if qps <= 0 || burst <= 0 {
		return nil
	}
	inner := flowcontrol.NewTokenBucketRateLimiter(qps, burst)
	if obs == nil {
		return inner
	}
	return CountingLimiter(inner, obs)
}

// IsolatedConfig copies cfg and installs a new token bucket.
// rest.CopyConfig copies RateLimiter by pointer, so a second clientset built
// from that copy waits on the first clientset's bucket. This copy does not.
// A nil cfg returns nil. Non-positive QPS or burst clears RateLimiter.
func IsolatedConfig(cfg *rest.Config, obs RequestObserver) *rest.Config {
	if cfg == nil {
		return nil
	}
	out := rest.CopyConfig(cfg)
	out.RateLimiter = FreshTokenBucket(out.QPS, out.Burst, obs)
	return out
}
