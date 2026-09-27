package metrics

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestCacheOpsPerRequest_CacheAsideMissIsTwoGetsPlusSet(t *testing.T) {
	t.Parallel()
	cache := ComponentMetrics{CacheStrategy: "cache-aside", CacheMetricMode: "keyspace hits/misses"}
	got := ReadPathCmdCountsFor(cache, "url-shortener")
	if got.HitRedisOps != 1 || got.MissRedisOps != 3 {
		t.Fatalf("cache-aside counts: %+v", got)
	}
	app := ComponentMetrics{CacheHits: 80, CacheMisses: 20}
	expected := ExpectedReadPathRedisOpsPerSec(app, cache, "url-shortener")
	want := 80.0*1 + 20.0*3
	if expected != want {
		t.Fatalf("expected %v got %v", want, expected)
	}
}

func TestCacheOpsPerRequest_ProxyMissIsGetPlusSet(t *testing.T) {
	t.Parallel()
	cache := ComponentMetrics{CacheStrategy: "write-through", CacheMetricMode: "layered"}
	got := ReadPathCmdCountsFor(cache, "url-shortener")
	if got.HitRedisOps != 1 || got.MissRedisOps != 2 {
		t.Fatalf("proxy counts: %+v", got)
	}
}

func TestCacheOpsPerRequest_ScanExcludedFromPerRequestExpected(t *testing.T) {
	t.Parallel()
	rates := map[string]float64{"get": 100, "set": 50, "scan": 200}
	got := RequestPathRedisOpsPerSec(rates)
	if got != 150 {
		t.Fatalf("request-path ops: got %v want 150", got)
	}
}

func TestBuildLedger_EmitsRedisCmdFactsInCacheAsideMode(t *testing.T) {
	t.Parallel()
	final := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {
				Component:   "app-server",
				RequestRate: 100,
				CacheHits:   90,
				CacheMisses: 10,
			},
			"cache": {
				Component:       "cache",
				CacheStrategy:   "cache-aside",
				CacheMetricMode: "keyspace hits/misses",
				RedisOpsPerSec:  120,
				RedisCmdRates: map[string]float64{
					"get": 80,
					"set": 40,
				},
			},
		},
	}
	l := BuildLedger(LedgerInput{
		DesignConfig: deployerDesignForCacheLedger(),
		Final:        final,
	})
	if l == nil {
		t.Fatal("ledger")
	}
	if _, ok := l.Resolve(FactRef("fact:derived:cache.opsPerAppRequest")); !ok {
		t.Fatal("missing opsPerAppRequest derived fact")
	}
	if _, ok := l.Resolve(FactRef("fact:metric:cache.redisOpsPerSec.last")); !ok {
		t.Fatal("missing redisOpsPerSec metric fact")
	}
	if _, ok := l.Resolve(FactRef("fact:metric:cache.redisCmd.get.last")); !ok {
		t.Fatal("missing redisCmd.get metric fact")
	}
}

func TestCacheOpsPerRequest_OmitsRatioBelowOneRPS(t *testing.T) {
	t.Parallel()
	final := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {
				Component:   "app-server",
				RequestRate: 0.5,
				CacheHits:   1,
				CacheMisses: 0,
			},
			"cache": {
				Component:      "cache",
				CacheStrategy:  "cache-aside",
				RedisOpsPerSec: 10,
				RedisCmdRates:  map[string]float64{"get": 10},
			},
		},
	}
	l := BuildLedger(LedgerInput{
		DesignConfig: deployerDesignForCacheLedger(),
		Final:        final,
	})
	if _, ok := l.Resolve(FactRef("fact:derived:cache.opsPerAppRequest")); ok {
		t.Fatal("opsPerAppRequest should be omitted below 1 rps")
	}
}

func deployerDesignForCacheLedger() deployer.DesignConfig {
	return deployer.DesignConfig{Template: "url-shortener"}
}
