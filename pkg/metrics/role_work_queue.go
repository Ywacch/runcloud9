package metrics

import (
	"context"
	"fmt"
	"strings"

	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/placement"
	"github.com/ywacch/cloud9/pkg/templates"
)

const streamsConsumerGroup = "cloud9-workers"

func queueNamesForTemplate(template string) []string {
	tmpl := templates.GetTemplate(template)
	if tmpl == nil {
		return nil
	}
	spec, ok := tmpl.Introspection["mq"]
	if !ok {
		return nil
	}
	return append([]string(nil), spec.Queues...)
}

func workQueueJob(topo *placement.Topology) (placement.Job, bool) {
	if topo == nil {
		return placement.Job{}, false
	}
	return topo.JobByID("work_queue")
}

func nodeHostsRabbitMQ(topo *placement.Topology, nodeID string) bool {
	if topo == nil {
		return true
	}
	node, ok := topo.NodeByID(nodeID)
	if !ok {
		return false
	}
	for _, m := range node.Members {
		if m.AppLabel == "rabbitmq" {
			return true
		}
	}
	return false
}

// collectWorkQueueMQ fills components["mq"] from the work_queue job adapter.
func (c *MetricsCollector) collectWorkQueueMQ(
	ctx context.Context,
	ns, template string,
	topo *placement.Topology,
	dm *DeploymentMetrics,
) {
	if c == nil || dm == nil {
		return
	}
	job, ok := workQueueJob(topo)
	if !ok {
		return
	}
	cm := ComponentMetrics{Component: "mq"}
	var gaps []SignalGap

	switch job.Engine {
	case "rabbitmq":
		if !nodeHostsRabbitMQ(topo, job.NodeID) {
			break
		}
		gaps = c.fillWorkQueueRabbitMQ(ctx, ns, job.NodeID, topo, &cm)
	case "redis":
		gaps = c.fillWorkQueueRedisStreams(ctx, ns, template, job, topo, &cm)
	default:
		return
	}

	if len(gaps) > 0 {
		if dm.SignalGaps == nil {
			dm.SignalGaps = make(map[string][]SignalGap)
		}
		mergeSignalGaps(dm.SignalGaps, "mq", gaps)
	}
	dm.Components["mq"] = cm
}

func (c *MetricsCollector) fillWorkQueueRabbitMQ(
	ctx context.Context,
	ns, nodeID string,
	topo *placement.Topology,
	cm *ComponentMetrics,
) []SignalGap {
	member, ok := topo.PrimaryMember(nodeID)
	if ok && member.AppLabel != "" {
		podRe := podReForAppLabel(member.AppLabel)
		container := member.Container
		if container == "" {
			container = "rabbitmq"
		}
		cpuRes := c.queryContainerResourceCPU(ctx, ns, podRe, container)
		cm.CPUUsage = cpuRes.Usage
		cm.CPUIsEstimated = cpuRes.Estimated
		cm.CPUSource = cpuRes.Source
		memRes := c.queryContainerResourceMemory(ctx, ns, podRe, container)
		cm.MemoryBytes = int64(memRes.Usage)
		cm.MemIsEstimated = memRes.Estimated
		cm.MemSource = memRes.Source
		cm.CPULimit = c.kubeLimitCPU(ctx, ns, podRe, container)
		cm.MemoryLimit = c.kubeLimitMem(ctx, ns, podRe, container)
	}
	c.applyRabbitMQBrokerMetrics(ctx, ns, cm)
	if cm.MQMemoryUsed > 0 {
		cm.MemoryBytes = int64(cm.MQMemoryUsed)
		cm.MemIsEstimated = false
		if cm.MQMemoryLimit > 0 {
			cm.MemoryLimit = int64(cm.MQMemoryLimit)
		}
	}
	mqPubRate, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_mq_publish_total",%s}%s))`, promNamespaceSelector(ns), promRateRange()))
	if mqPubRate > 0 && cm.ProduceRate == 0 {
		cm.ProduceRate = mqPubRate
	}
	pubTier1, _ := c.query(ctx, fmt.Sprintf(`sum(rate(queue_publish_total{%s}%s))`, promNamespaceSelector(ns), promRateRange()))
	if pubTier1 > 0 {
		cm.ProduceRate = pubTier1
	}
	workerRPS, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_worker_processed_total",%s}%s))`, promNamespaceSelector(ns), promRateRange()))
	if workerRPS > 0 {
		cm.ConsumeRate = workerRPS
	}
	ackTier1, _ := c.query(ctx, fmt.Sprintf(`sum(rate(queue_ack_total{%s}%s))`, promNamespaceSelector(ns), promRateRange()))
	if ackTier1 > 0 {
		cm.ConsumeRate = ackTier1
	}
	if cm.ConsumeRate > 0 {
		cm.MQSaturationRatio = cm.ProduceRate / cm.ConsumeRate
	} else if cm.ProduceRate > 0 {
		cm.MQSaturationRatio = 10
	}
	return nil
}

func (c *MetricsCollector) fillWorkQueueRedisStreams(
	ctx context.Context,
	ns, template string,
	job placement.Job,
	topo *placement.Topology,
	cm *ComponentMetrics,
) []SignalGap {
	var gaps []SignalGap
	member, ok := topo.PrimaryMember(job.NodeID)
	if !ok {
		return gaps
	}
	app := member.AppLabel
	sel := promAppSelector(ns, app)
	queues := queueNamesForTemplate(template)
	if len(queues) == 0 {
		return gaps
	}

	var readySum, pendingSum float64
	var readyOK, pendingOK bool
	for _, stream := range queues {
		stream = strings.TrimSpace(stream)
		if stream == "" {
			continue
		}
		lagQ := fmt.Sprintf(
			`sum(redis_stream_group_lag{%s,stream=%q,group=%q})`,
			sel, stream, streamsConsumerGroup,
		)
		lag, present, _ := c.queryScalar(ctx, lagQ)
		if present {
			readySum += lag
			readyOK = true
		}
		pendQ := fmt.Sprintf(
			`sum(redis_stream_group_messages_pending{%s,stream=%q,group=%q})`,
			sel, stream, streamsConsumerGroup,
		)
		pend, present, _ := c.queryScalar(ctx, pendQ)
		if present {
			pendingSum += pend
			pendingOK = true
		}
	}
	if readyOK {
		cm.MessagesReady = readySum
	} else {
		gaps = append(gaps, gapNotBuilt("messagesReady", "redis_stream_group_lag"))
	}
	if pendingOK {
		cm.MessagesUnacked = pendingSum
	} else {
		gaps = append(gaps, gapNotBuilt("messagesUnacked", "redis_stream_group_messages_pending"))
	}
	if readyOK && pendingOK {
		cm.QueueDepth = readySum + pendingSum
	}
	pub, _ := c.query(ctx, fmt.Sprintf(`sum(rate(queue_publish_total{%s}%s))`, sel, promRateRange()))
	if pub > 0 {
		cm.ProduceRate = pub
	} else {
		legacyPub, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_mq_publish_total",%s}%s))`, promNamespaceSelector(ns), promRateRange()))
		if legacyPub > 0 {
			cm.ProduceRate = legacyPub
		}
	}
	ack, _ := c.query(ctx, fmt.Sprintf(`sum(rate(queue_ack_total{%s}%s))`, sel, promRateRange()))
	if ack > 0 {
		cm.ConsumeRate = ack
	} else {
		workerRPS, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_worker_processed_total",%s}%s))`, promNamespaceSelector(ns), promRateRange()))
		if workerRPS > 0 {
			cm.ConsumeRate = workerRPS
		}
	}
	if cm.ConsumeRate > 0 {
		cm.MQSaturationRatio = cm.ProduceRate / cm.ConsumeRate
	} else if cm.ProduceRate > 0 {
		cm.MQSaturationRatio = 10
	}

	cat := engines.Default()
	if pair, found := cat.Pair("work_queue", "redis"); found {
		for _, sig := range pair.Signals {
			if sig.Availability != engines.AvailabilityNotApplicable {
				continue
			}
			gaps = append(gaps, gapFromPair(sig.Field, sig.Availability, sig.Reason, sig.Concept))
		}
	}
	return gaps
}
