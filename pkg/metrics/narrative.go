package metrics

import (
	"fmt"
	"strings"

	"github.com/ywacch/cloud9/pkg/templates"
)

// NarrativeOutput holds storytelling prose for a single detector rule.
type NarrativeOutput struct {
	Narrative  string
	Suggestion string
	Related    []string
}

// Narrate generates cause→effect narrative and topology-only canvas suggestions for a rule id.
// rule must match constants from threshold and cascade detectors (e.g. ruleCacheHitLow, cascadeRuleCacheDB).
// tmpl may be nil; when set, template YAML narrativeContext overlays generic metric prose.
func Narrate(rule string, dm *DeploymentMetrics, topo TopologyFlags, tmpl *templates.TemplateSpec) NarrativeOutput {
	if dm == nil {
		return NarrativeOutput{}
	}
	switch rule {
	case ruleCacheHitLow:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateCacheHitLow(dm, topo))
	case ruleErrorRate:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateErrorRate(dm, topo))
	case ruleDBLatency:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateDBLatency(dm, topo))
	case ruleQueueDepth:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateQueueDepth(dm, topo))
	case ruleDBConnPressure:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateDBConnPressure(dm, topo))
	case ruleMQBackpressure:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateMQBackpressure(dm, topo))
	case cascadeRuleCacheDB:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateCascadeCacheDB(dm, topo))
	case cascadeRuleQueueWorker:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateCascadeQueueWorker(dm, topo))
	case cascadeRuleDBPoolErrors:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateCascadeDBPoolErrors(dm, topo))
	case cascadeRuleLatencyMiss:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateCascadeLatencyMiss(dm, topo))
	case cascadeRuleMQBackpressure:
		return applyNarrativeFromTemplate(rule, dm, tmpl, narrateCascadeMQBackpressure(dm, topo))
	case rulePodOOM:
		return narratePodOOM(nil)
	case rulePodRestart:
		return narratePodRestart(nil)
	default:
		return NarrativeOutput{}
	}
}

func applyNarrativeFromTemplate(rule string, dm *DeploymentMetrics, tmpl *templates.TemplateSpec, base NarrativeOutput) NarrativeOutput {
	if tmpl == nil || tmpl.Storytelling == nil || tmpl.Storytelling.NarrativeContext == nil {
		return base
	}
	ctx, ok := tmpl.Storytelling.NarrativeContext[rule]
	if !ok {
		return base
	}
	out := base
	if ctx.Cause != "" {
		extra := strings.TrimSpace(interpolateNarrativePlaceholders(ctx.Cause, dm))
		if extra != "" {
			if out.Narrative != "" {
				out.Narrative = strings.TrimSpace(out.Narrative + " " + extra)
			} else {
				out.Narrative = extra
			}
		}
	}
	if ctx.Suggestion != "" {
		out.Suggestion = strings.TrimSpace(interpolateNarrativePlaceholders(ctx.Suggestion, dm))
	}
	return out
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
	fmt.Fprintf(&b,
		"The rolling ~9 minute cache hit rate is %.1f%% based on hit/miss counters in Prometheus. "+
			"When this drops, more read paths miss Redis and fall through to the database. ",
		hr)
	if total > 0 {
		fmt.Fprintf(&b, "In this window there were about %.0f hits and %.0f misses.", cache.CacheHitsLast9m, cache.CacheMissesLast9m)
	} else {
		b.WriteString("Hit/miss counters are not populated enough yet to quantify the window.")
	}
	if db.DbQueryRate > 0 {
		fmt.Fprintf(&b, " DB query rate is about %.1f/s, so Postgres may be absorbing work that would be cheaper on Redis.", db.DbQueryRate)
	}

	sug := "On the canvas, ensure a cache node is present and sized for your working set; if eviction is likely, add Redis memory or scale out cache capacity."
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
	b.WriteString("Higher averages usually mean heavier queries, lock contention, or the database running out of CPU or I/O headroom.")

	related := []string{"db"}
	if !topo.HasCache {
		related = append(related, "cache")
	}

	sug := "Add a cache node and route hot reads through it, or add read replicas so query load is split across more Postgres instances."

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    related,
	}
}

func narrateQueueDepth(dm *DeploymentMetrics, topo TopologyFlags) NarrativeOutput {
	mq := dm.Components["mq"]

	var b strings.Builder
	fmt.Fprintf(&b, "%s queue depth is about %.0f messages. ", WorkQueueEngineLabel(dm), mq.QueueDepth)
	fmt.Fprintf(&b, "Publish rate is about %.1f/s and consume rate about %.1f/s. ", mq.ProduceRate, mq.ConsumeRate)
	if mq.ConsumeRate > 0.01 {
		est := mq.QueueDepth / mq.ConsumeRate
		fmt.Fprintf(&b, "At the current consume rate, draining the visible backlog would take on the order of %.0f seconds if producers paused.", est)
	} else {
		b.WriteString("Consume rate is near zero or unknown, so backlog may persist until consumers come online.")
	}

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
	fmt.Fprintf(&b, "Postgres reports about %.0f open connections out of %.0f max (%.1f%%). ", db.DbActiveConnections, db.MaxConnections, pct)
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

	var b strings.Builder
	fmt.Fprintf(&b, "Producers are ahead of consumers: saturation ratio is about %.2f with queue depth about %.0f. ", mq.MQSaturationRatio, mq.QueueDepth)
	fmt.Fprintf(&b, "Publish-side rate is about %.1f/s and consume-side rate about %.1f/s. ", prod, cons)

	b.WriteString("When publish rate exceeds consume rate, depth grows and downstream writers (for example workers updating a store) can fall behind.")

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

func narrateCascadeCacheDB(dm *DeploymentMetrics, _ TopologyFlags) NarrativeOutput {
	cache := dm.Components["cache"]
	db := dm.Components["db"]
	hr := lifetimeHitRate(cache)

	var b strings.Builder
	fmt.Fprintf(&b, "Cache hit rate is about %.1f%% while DB query rate is about %.1f/s. ", hr, db.DbQueryRate)
	b.WriteString("When hit rate drops and DB rate rises together, Postgres is often doing work that would have been cheap on Redis.")

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: "On the canvas, add Redis memory or cache capacity if hot keys are being evicted, or add read replicas if the database is the sustained bottleneck.",
		Related:    []string{"cache", "db"},
	}
}

func narrateCascadeQueueWorker(dm *DeploymentMetrics, _ TopologyFlags) NarrativeOutput {
	mq := dm.Components["mq"]
	w := dm.Components["worker"]

	var b strings.Builder
	fmt.Fprintf(&b, "Queue depth is about %.0f while worker CPU is about %.3f cores. ", mq.QueueDepth, w.CPUUsage)
	b.WriteString("Depth growing together with worker CPU usually means consumers are busy but still not keeping up with arrival rate, which is common when each message triggers many writes.")

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: "Add more worker replicas so fan-out and queue draining can run in parallel across the topology.",
		Related:    []string{"mq", "worker"},
	}
}

func narrateCascadeDBPoolErrors(dm *DeploymentMetrics, _ TopologyFlags) NarrativeOutput {
	db := dm.Components["db"]
	app := dm.Components["app-server"]

	var b strings.Builder
	if db.MaxConnections > 0 {
		fmt.Fprintf(&b, "Connections are about %.0f of %.0f max while HTTP errors are about %.2f/s. ", db.DbActiveConnections, db.MaxConnections, app.ErrorRate)
	} else {
		fmt.Fprintf(&b, "Database connections are elevated while HTTP errors are about %.2f/s. ", app.ErrorRate)
	}
	b.WriteString("When the pool is exhausted, new requests cannot acquire a session and the app-server often returns errors to clients.")

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: "Add a cache node to cut read volume on the primary, or add read replicas so fewer sessions pile onto a single Postgres instance.",
		Related:    []string{"db", "app-server"},
	}
}

func narrateCascadeLatencyMiss(dm *DeploymentMetrics, _ TopologyFlags) NarrativeOutput {
	app := dm.Components["app-server"]
	p99ms := app.P99Latency * 1000

	var b strings.Builder
	fmt.Fprintf(&b, "App p99 latency is about %.0f ms while cache miss share in the rolling window is elevated. ", p99ms)
	b.WriteString("That pattern usually means more requests miss Redis and pay the full database latency stack.")

	sug := "Add or enlarge the cache tier so hot keys stay resident, or add read replicas if the database dominates latency."

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    []string{"app-server", "cache", "db"},
	}
}

func narrateCascadeMQBackpressure(dm *DeploymentMetrics, _ TopologyFlags) NarrativeOutput {
	mq := dm.Components["mq"]
	prod := mq.ProduceRate
	cons := mq.ConsumeRate

	var b strings.Builder
	fmt.Fprintf(&b, "MQ saturation is about %.2f with depth about %.0f. ", mq.MQSaturationRatio, mq.QueueDepth)
	fmt.Fprintf(&b, "Produce is about %.1f/s versus consume about %.1f/s, so backlog builds when that gap persists over consecutive ticks.", prod, cons)

	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: "Add worker replicas to raise aggregate consume throughput.",
		Related:    []string{"mq", "worker", "cache"},
	}
}

func narratePodOOM(ctx *CrashContext) NarrativeOutput {
	var memPct float64
	if ctx != nil {
		memPct = ctx.MemPercent
	}
	var b strings.Builder
	b.WriteString("Kubernetes terminated the container with OOMKilled — it exceeded its cgroup memory limit.")
	if memPct >= 85 {
		fmt.Fprintf(&b, " Memory utilization was about %.0f%% of limit on the last metrics tick.", memPct)
	}
	sug := "Raise the instance size for this component or reduce load until memory stays below the limit."
	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: sug,
		Related:    []string{"cache", "app-server", "db"},
	}
}

func narratePodRestart(ctx *CrashContext) NarrativeOutput {
	var b strings.Builder
	b.WriteString("A pod restarted during the live session.")
	if ctx != nil {
		if ctx.WaitingReason == "CrashLoopBackOff" {
			b.WriteString(" It is in CrashLoopBackOff — Kubernetes is backing off after repeated failures.")
		}
		if ctx.ErrorRate > 1 {
			fmt.Fprintf(&b, " App error rate was about %.1f/s on the same tick.", ctx.ErrorRate)
		}
		if ctx.ActiveConnections > 0 && ctx.ActiveConnections > 500 {
			fmt.Fprintf(&b, " Load balancer active connections were about %.0f.", ctx.ActiveConnections)
		}
		if ctx.RedisRejectedRate > 0 {
			fmt.Fprintf(&b, " Redis rejected about %.0f connections/s.", ctx.RedisRejectedRate)
		}
		if ctx.QueueDepth > 50 {
			fmt.Fprintf(&b, " Queue depth was about %.0f.", ctx.QueueDepth)
		}
	}
	return NarrativeOutput{
		Narrative:  strings.TrimSpace(b.String()),
		Suggestion: "Check pod logs and whether CPU, memory, or upstream health checks failed under load.",
		Related:    []string{"app-server", "cache", "lb"},
	}
}
