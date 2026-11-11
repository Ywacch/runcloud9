package kubeobs

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/rest"
)

func TestFreshTokenBucket_SeparateBuckets(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	a := FreshTokenBucket(0.01, 1, nil)
	b := FreshTokenBucket(0.01, 1, nil)
	if a == nil || b == nil {
		t.Fatal("expected limiters")
	}
	if err := a.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("second bucket waited %s on the first bucket", elapsed)
	}
}

func TestFreshTokenBucket_NonPositive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		qps   float32
		burst int
	}{
		{name: "zero qps", qps: 0, burst: 10},
		{name: "zero burst", qps: 10, burst: 0},
		{name: "negative qps", qps: -1, burst: 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FreshTokenBucket(tc.qps, tc.burst, nil); got != nil {
				t.Fatalf("limiter = %v, want nil", got)
			}
		})
	}
}

func TestIsolatedConfig_ReplacesSharedLimiter(t *testing.T) {
	t.Parallel()

	shared := FreshTokenBucket(10, 20, nil)
	cfg := &rest.Config{
		Host:        "https://example.com",
		QPS:         10,
		Burst:       20,
		RateLimiter: shared,
	}
	out := IsolatedConfig(cfg, nil)
	if out == nil || out.RateLimiter == nil {
		t.Fatal("expected a copied config with a limiter")
	}
	if out.RateLimiter == cfg.RateLimiter {
		t.Fatal("copy kept the shared limiter")
	}
	if cfg.RateLimiter != shared {
		t.Fatal("source limiter changed")
	}
	if out.QPS != 10 || out.Burst != 20 {
		t.Fatalf("qps/burst = %v/%d, want 10/20", out.QPS, out.Burst)
	}
}

func TestIsolatedConfig_ClearsLimiterWhenUnset(t *testing.T) {
	t.Parallel()

	shared := FreshTokenBucket(10, 20, nil)
	cfg := &rest.Config{QPS: 0, Burst: 0, RateLimiter: shared}
	out := IsolatedConfig(cfg, nil)
	if out == nil {
		t.Fatal("expected a copy")
	}
	if out.RateLimiter != nil {
		t.Fatal("expected nil limiter when qps and burst are unset")
	}
	if cfg.RateLimiter != shared {
		t.Fatal("source limiter changed")
	}
}

func TestIsolatedConfig_Nil(t *testing.T) {
	t.Parallel()
	if IsolatedConfig(nil, nil) != nil {
		t.Fatal("expected nil")
	}
}
