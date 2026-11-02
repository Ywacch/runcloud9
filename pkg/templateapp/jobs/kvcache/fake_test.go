package kvcache_test

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/kvcache"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/kvcache/kvcachetest"
)

func TestFake_Conformance(t *testing.T) {
	t.Parallel()
	kvcachetest.RunConformance(t, func(t *testing.T) (kvcache.Cache, func()) {
		t.Helper()
		return kvcachetest.NewFake(nil), func() {}
	})
}

func TestFake_TTLAndLockExpiry(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	clock := now
	advance := func(d time.Duration) {
		clock = clock.Add(d)
	}
	f := kvcachetest.NewFake(func() time.Time { return clock })
	kvcachetest.RunTTLConformance(t, func(t *testing.T) (kvcache.Cache, func()) {
		t.Helper()
		return f, func() {}
	}, advance)
	kvcachetest.RunLockExpiryConformance(t, func(t *testing.T) (kvcache.Cache, func()) {
		t.Helper()
		return f, func() {}
	}, advance)
}
