package metrics

// appendDerivedCacheFacts emits derived cache command facts from the final metrics snapshot.
func appendDerivedCacheFacts(l *Ledger, final *DeploymentMetrics, template string) {
	if l == nil || final == nil {
		return
	}
	app, okApp := final.Components["app-server"]
	cache, okCache := final.Components["cache"]
	if !okApp || !okCache {
		return
	}

	reqOps := cache.RedisOpsPerSec
	if len(cache.RedisCmdRates) > 0 {
		reqOps = RequestPathRedisOpsPerSec(cache.RedisCmdRates)
	}
	scanRate := cache.RedisCmdRates["scan"]
	if cache.CacheStrategy == "write-behind" && scanRate > 0 {
		l.addFact(Fact{
			Ref:       FactRef("fact:derived:cache.flusherOpsPerSec"),
			Kind:      FactKindDerived,
			Component: "cache",
			Value: map[string]any{
				"window":        PromRateWindowLabel,
				"scanOpsPerSec": scanRate,
			},
		})
	}

	expectedOps := ExpectedReadPathRedisOpsPerSec(app, cache, template)
	traffic := app.CacheHits + app.CacheMisses
	if traffic > 0 && (reqOps > 0 || expectedOps > 0) {
		l.addFact(Fact{
			Ref:       FactRef("fact:derived:cache.readPathResidualOpsPerSec"),
			Kind:      FactKindDerived,
			Component: "cache",
			Value: map[string]any{
				"window":                       PromRateWindowLabel,
				"observedRequestPathOpsPerSec": reqOps,
				"expectedReadPathOpsPerSec":    expectedOps,
				"residualOpsPerSec":            reqOps - expectedOps,
				"note":                         "residual after read-path model; writes and flusher GET/DEL can appear here",
			},
		})
	}

	appRPS := app.RequestRate
	if appRPS < minAppRequestRateForCacheRatio {
		return
	}
	if reqOps <= 0 {
		return
	}

	l.addFact(Fact{
		Ref:       FactRef("fact:derived:cache.opsPerAppRequest"),
		Kind:      FactKindDerived,
		Component: "cache",
		Value: map[string]any{
			"window":                    PromRateWindowLabel,
			"requestPathRedisOpsPerSec": reqOps,
			"appRequestRate":            appRPS,
			"opsPerCompletedAppRequest": reqOps / appRPS,
		},
	})

	if expectedOps <= 0 {
		return
	}
	readCounts := ReadPathCmdCountsFor(cache, template)
	l.addFact(Fact{
		Ref:       FactRef("fact:derived:cache.expectedOpsPerAppRequest"),
		Kind:      FactKindDerived,
		Component: "cache",
		Value: map[string]any{
			"source":                    "model",
			"covers":                    "read_path",
			"window":                    PromRateWindowLabel,
			"expectedReadPathOpsPerSec": expectedOps,
			"appRequestRate":            appRPS,
			"opsPerCompletedAppRequest": expectedOps / appRPS,
			"hitRedisOpsPerRead":        readCounts.HitRedisOps,
			"missRedisOpsPerRead":       readCounts.MissRedisOps,
		},
	})
}
