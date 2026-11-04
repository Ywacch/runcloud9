package kubeobs

import (
	"context"
	"time"

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
