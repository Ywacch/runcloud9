package metrics

import (
	"fmt"
	"strings"
)

// NarrativeOutput holds storytelling prose for a single detector rule.
type NarrativeOutput struct {
	Narrative  string
	Suggestion string
	Related    []string
}

// Narrate generates cause→effect narrative and topology-only canvas suggestions for a rule id.
// rule must match constants from threshold and cascade detectors (e.g. ruleCacheHitLow, cascadeRuleCacheDB).
func Narrate(rule string, dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	if dm == nil {
		return NarrativeOutput{}
	}
	switch rule {
	case ruleCacheHitLow:
		return narrateCacheHitLow(dm, topo)
	case ruleErrorRate:
		return narrateErrorRate(dm, topo)
	case ruleDBLatency:
		return narrateDBLatency(dm, topo)
	case ruleQueueDepth:
		return narrateQueueDepth(dm, topo)
	case ruleDBConnPressure:
		return narrateDBConnPressure(dm, topo)
	case ruleMQBackpressure:
		return narrateMQBackpressure(dm, topo)
	case cascadeRuleCacheDB:
		return narrateCascadeCacheDB(dm, topo)
	case cascadeRuleQueueWorker:
		return narrateCascadeQueueWorker(dm, topo)
	case cascadeRuleDBPoolErrors:
		return narrateCascadeDBPoolErrors(dm, topo)
	case cascadeRuleLatencyMiss:
		return narrateCascadeLatencyMiss(dm, topo)
	case cascadeRuleMQBackpressure:
		return narrateCascadeMQBackpressure(dm, topo)
	default:
		return NarrativeOutput{}
	}
}

func tmplFloat(dm *DeploymentMetrics, key string) float64 {
	if dm == nil || dm.TemplateMetrics == nil {
		return 0
	}
	return dm.TemplateMetrics[key]
}

func narrateCacheHitLow(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	cache := dm.Components["cache"]
	db := dm.Components["db"]
	hr := lifetimeHitRate(cache)
	total := cache.CacheHitsLast9m + cache.CacheMissesLast9m

	var b strings.Builder
	if topo.CacheStrategy == "timeline-store" || strings.EqualFold(topo.Template, "social-feed") {
		fmt.Fprintf(&b,
			"The rolling ~9 minute window shows a low hit share (%.1f%%) for timeline-style Redis traffic. "+
				"Sorted-set timelines are not classic cache-aside GET/SET; a low ratio can mean cold timelines, eviction under memory pressure, or traffic patterns that rarely reuse keys. ",
			hr)
	} else {
		fmt.Fprintf(&b,
			"The rolling ~9 minute cache hit rate is %.1f%% based on hit/miss counters in Prometheus. "+
				"When this drops, more read paths miss Redis and fall through to the database. ",
			hr)
	}
	if total > 0 {
		fmt.Fprintf(&b, "In this window there were about %.0f hits and %.0f misses.", cache.CacheHitsLast9m, cache.CacheMissesLast9m)
	} else {
		b.WriteString("Hit/miss counters are not populated enough yet to quantify the window.")
	}
	if db.DbQueryRate > 0 {
		fmt.Fprintf(&b, " DB query rate is about %.1f/s, so Postgres may be absorbing work that would be cheaper on Redis.", db.DbQueryRate)
	}
	b.WriteString(".")

	sug := "On the canvas, ensure a cache node is present and sized for your working set; if eviction is likely, add Redis memory or shard timelines across more cache capacity."
	if !topo.HasCache {
		sug = "Add a cache node to the topology so hot reads can be served without hitting the database on every request."
	}

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    []string{"cache", "db"},
	}
}

func narrateErrorRate(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	app := dm.Components["app-server"]
	db := dm.Components["db"]
	cache := dm.Components["cache"]

	var b strings.Builder
	fmt.Fprintf(&b, "The app-server is reporting about %.2f HTTP 5xx errors per second while serving roughly %.1f req/s. ", app.ErrorRate, app.RequestRate)
	if db.DbQueryLatency > 0.25 {
		b.WriteString("Database latency is elevated, which can surface as upstream errors when queries time out.")
	} else if cache.RedisRejectedRate > 0 {
		b.WriteString("Redis is rejecting or erroring connections at a non-zero rate, which can break read paths.")
	} else {
		b.WriteString("Downstream saturation or application faults often show up first as rising 5xx on the edge.")
	}
	b.WriteString(".")

	related := []string{"app-server"}
	if topo.HasCache || cache.RedisOpsPerSec > 0 {
		related = append(related, "cache")
	}
	if db.DbQueryRate > 0 || db.DbActiveConnections > 0 {
		related = append(related, "db")
	}

	sug := "On the canvas, add more app-server replicas behind the load balancer (or add an LB node) to spread traffic, and add or scale a cache tier if reads are overloading Postgres."
	if topo.HasLB {
		sug = "Increase app-server replicas behind the load balancer and add or scale supporting tiers (cache, DB read capacity) that match where latency or saturation shows up."
	}

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    related,
	}
}

func narrateDBLatency(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	db := dm.Components["db"]
	latMs := db.DbQueryLatency * 1000

	var b strings.Builder
	fmt.Fprintf(&b, "Average database query latency is about %.0f ms. ", latMs)
	switch topo.Template {
	case "social-feed", "social-feed-pull":
		b.WriteString("For social-feed read paths that still hit Postgres, work often involves joining posts, follows, and users, which can get expensive as data grows.")
	default:
		b.WriteString("Higher averages usually mean heavier queries, lock contention, or the database running out of CPU or I/O headroom.")
	}
	b.WriteString(".")

	related := []string{"db"}
	if !topo.HasCache {
		related = append(related, "cache")
	}

	sug := "Add a cache node and route hot reads through it, or add read replicas so query load is split across more Postgres instances."
	if topo.Template == "social-feed" || topo.Template == "social-feed-pull" {
		sug = "Add a cache node with the timeline-store pattern and wire MQ plus worker nodes so timelines are precomputed; reads then come from Redis instead of large joins on every request."
	}

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    related,
	}
}

func narrateQueueDepth(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	mq := dm.Components["mq"]

	var b strings.Builder
	fmt.Fprintf(&b, "RabbitMQ queue depth is about %.0f messages. ", mq.QueueDepth)
	fmt.Fprintf(&b, "Publish rate is about %.1f/s and consume rate about %.1f/s. ", mq.ProduceRate, mq.ConsumeRate)
	if mq.ConsumeRate > 0.01 {
		est := mq.QueueDepth / mq.ConsumeRate
		fmt.Fprintf(&b, "At the current consume rate, draining the visible backlog would take on the order of %.0f seconds if producers paused.", est)
	} else {
		b.WriteString("Consume rate is near zero or unknown, so backlog may persist until consumers come online.")
	}
	b.WriteString(".")

	sug := "Add more worker replicas so the topology can drain the queue faster."
	if !topo.HasWorker {
		sug = "Add a worker node connected to the message queue so messages are not stuck without consumers."
	}

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    []string{"mq", "worker"},
	}
}

func narrateDBConnPressure(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	db := dm.Components["db"]
	app := dm.Components["app-server"]
	pct := 0.0
	if db.MaxConnections > 0 {
		pct = 100 * db.DbActiveConnections / db.MaxConnections
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Postgres reports about %.0f active connections out of %.0f max (%.1f%%). ", db.DbActiveConnections, db.MaxConnections, pct)
	fmt.Fprintf(&b, "The app-server is offering roughly %.1f req/s, so pool contention can surface as timeouts once the database stops accepting new sessions.", app.RequestRate)

	sug := "Add a cache node to shrink read load on the primary, or add read replicas so connections and queries spread across more database instances."
	if topo.HasCache {
		sug = "Add read replicas or more app-server replicas so fewer sessions pile onto the primary, and keep the cache tier effective so reads do not amplify connection usage."
	}

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    []string{"db", "app-server"},
	}
}

func narrateMQBackpressure(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	mq := dm.Components["mq"]
	prod := tmplFloat(dm, "mq.produce_rate")
	if prod <= 0 {
		prod = mq.ProduceRate
	}
	cons := tmplFloat(dm, "worker.consume_rate")
	if cons <= 0 {
		cons = mq.ConsumeRate
	}
	fanout := tmplFloat(dm, "worker.fanout_size_p99")
	redisWrites := tmplFloat(dm, "worker.redis_write_rate")

	var b strings.Builder
	fmt.Fprintf(&b, "Producers are ahead of consumers: saturation ratio is about %.2f with queue depth about %.0f. ", mq.MQSaturationRatio, mq.QueueDepth)
	fmt.Fprintf(&b, "Publish-side rate is about %.1f/s and consume-side rate about %.1f/s. ", prod, cons)

	if strings.EqualFold(topo.Template, "social-feed") && (fanout > 0 || redisWrites > 0 || prod > 0) {
		if fanout > 0 && prod > 0 && redisWrites > 0 {
			fmt.Fprintf(&b,
				"For social-feed, p99 fan-out is about %.0f followers per post while publish rate is about %.1f posts/s; Redis write rate from workers is about %.1f/s, "+
					"so each post can amplify to many timeline writes before readers see warm timelines.",
				fanout, prod, redisWrites)
		} else {
			b.WriteString("For social-feed, each published post can fan out to many follower timelines, so modest publish rates can still create large Redis write volume.")
		}
	} else {
		b.WriteString("When publish rate exceeds consume rate, depth grows and downstream writers (for example workers updating a store) can fall behind.")
	}
	b.WriteString(".")

	sug := "Add more worker replicas to increase consume and fan-out throughput."
	if !topo.HasWorker {
		sug = "Add worker nodes connected to the queue so published work is consumed in parallel."
	}

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    []string{"mq", "worker", "cache"},
	}
}

func narrateCascadeCacheDB(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	cache := dm.Components["cache"]
	db := dm.Components["db"]
	hr := lifetimeHitRate(cache)

	var b strings.Builder
	fmt.Fprintf(&b, "Cache hit rate is about %.1f%% while DB query rate is about %.1f/s. ", hr, db.DbQueryRate)
	if topo.CacheStrategy == "timeline-store" {
		b.WriteString("When timeline freshness or eviction shifts what is resident in Redis, read paths can lean harder on Postgres even though the data model is push-oriented.")
	} else {
		b.WriteString("When hit rate drops and DB rate rises together, Postgres is often doing work that would have been cheap on Redis.")
	}
	b.WriteString(".")

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: "On the canvas, add Redis memory or cache capacity if timelines or keys are being evicted, or add read replicas if the database is the sustained bottleneck.",
		Related:    []string{"cache", "db"},
	}
}

func narrateCascadeQueueWorker(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	mq := dm.Components["mq"]
	w := dm.Components["worker"]

	var b strings.Builder
	fmt.Fprintf(&b, "Queue depth is about %.0f while worker CPU is about %.3f cores. ", mq.QueueDepth, w.CPUUsage)
	b.WriteString("Depth growing together with worker CPU usually means consumers are busy but still not keeping up with arrival rate, which is common when each message triggers many writes.")
	b.WriteString(".")

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: "Add more worker replicas so fan-out and queue draining can run in parallel across the topology.",
		Related:    []string{"mq", "worker"},
	}
}

func narrateCascadeDBPoolErrors(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	db := dm.Components["db"]
	app := dm.Components["app-server"]

	var b strings.Builder
	if db.MaxConnections > 0 {
		fmt.Fprintf(&b, "Connections are about %.0f of %.0f max while HTTP errors are about %.2f/s. ", db.DbActiveConnections, db.MaxConnections, app.ErrorRate)
	} else {
		fmt.Fprintf(&b, "Database connections are elevated while HTTP errors are about %.2f/s. ", app.ErrorRate)
	}
	b.WriteString("When the pool is exhausted, new requests cannot acquire a session and the app-server often returns errors to clients.")
	b.WriteString(".")

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: "Add a cache node to cut read volume on the primary, or add read replicas so fewer sessions pile onto a single Postgres instance.",
		Related:    []string{"db", "app-server"},
	}
}

func narrateCascadeLatencyMiss(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	app := dm.Components["app-server"]
	p99ms := app.P99Latency * 1000

	var b strings.Builder
	fmt.Fprintf(&b, "App p99 latency is about %.0f ms while cache miss share in the rolling window is elevated. ", p99ms)
	if topo.CacheStrategy == "timeline-store" {
		b.WriteString("For timeline-store, rising latency with more misses often means timelines are cold or incomplete and read paths are doing more expensive work than a hot ZREVRANGE path.")
	} else {
		b.WriteString("That pattern usually means more requests miss Redis and pay the full database latency stack.")
	}
	b.WriteString(".")

	sug := "Add or enlarge the cache tier so hot keys stay resident, or add read replicas if the database dominates latency."
	if strings.EqualFold(topo.Template, "social-feed") {
		sug = "Wire MQ and worker nodes so timelines are precomputed into Redis; keep the cache node so reads stay on sorted-set timelines instead of large joins."
	}

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    []string{"app-server", "cache", "db"},
	}
}

func narrateCascadeMQBackpressure(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	mq := dm.Components["mq"]
	prod := mq.ProduceRate
	cons := mq.ConsumeRate

	var b strings.Builder
	fmt.Fprintf(&b, "MQ saturation is about %.2f with depth about %.0f. ", mq.MQSaturationRatio, mq.QueueDepth)
	fmt.Fprintf(&b, "Produce is about %.1f/s versus consume about %.1f/s, so backlog builds when that gap persists over consecutive ticks.", prod, cons)
	if strings.EqualFold(topo.Template, "social-feed") {
		b.WriteString(" On social-feed, sustained gaps often mean fan-out writes to Redis cannot keep up with posting load.")
	}
	b.WriteString(".")

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: "Add worker replicas to raise aggregate consume throughput, and ensure a cache node exists for the timeline store that workers write into.",
		Related:    []string{"mq", "worker", "cache"},
	}
}
