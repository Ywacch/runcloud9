package deployer

import (
	"github.com/ywacch/cloud9/pkg/templates"
)

// splitQueueCache reports a valid two-cache layout: Redis work_queue on one cache
// node and timeline or hot_cache on the other.
func splitQueueCache(design DeploymentDesign) (queueNodeID, dataCacheID string, ok bool) {
	cacheCount := 0
	for _, n := range design.Nodes {
		if n.Type == "cache" {
			cacheCount++
		}
	}
	if cacheCount != 2 {
		return "", "", false
	}
	tmpl := templates.GetTemplate(design.Template)
	if tmpl == nil || len(tmpl.Jobs) == 0 {
		return "", "", false
	}
	bindings := ResolveJobBindings(design, tmpl)
	var queueID, dataID string
	for _, b := range bindings {
		if b.NodeType != "cache" {
			continue
		}
		switch b.Job {
		case "work_queue":
			if b.Engine != "redis" {
				return "", "", false
			}
			queueID = b.NodeID
		case "timeline", "hot_cache":
			dataID = b.NodeID
		}
	}
	if queueID == "" || dataID == "" || queueID == dataID {
		return "", "", false
	}
	return queueID, dataID, true
}

// redisServiceHost is the K8s DNS name for a cache node's Redis service.
func redisServiceHost(nodeID string, design DeploymentDesign) string {
	if queueID, _, split := splitQueueCache(design); split && nodeID == queueID {
		return "work-queue"
	}
	return "cache"
}

// redisCacheDeployIdentity names the Redis Deployment, Service, and app label for a cache node.
func redisCacheDeployIdentity(node DeploymentNode, design DeploymentDesign) (depName, svcName, appLabel string) {
	if queueID, _, split := splitQueueCache(design); split && node.ID == queueID {
		return "work-queue", "work-queue", "work-queue"
	}
	return "redis", "cache", "redis"
}

// redisPolicyForNode picks maxmemory-policy for a cache-aside Redis pod.
func redisPolicyForNode(node DeploymentNode, design DeploymentDesign) string {
	canvas := normalizeRedisEvictionPolicy(configString(node.Config, "evictionPolicy", "allkeys-lru"))
	tmpl := templates.GetTemplate(design.Template)
	if tmpl == nil {
		return canvas
	}
	bindings := ResolveJobBindings(design, tmpl)
	hasQueue := false
	hasDataJob := false
	for _, b := range bindings {
		if b.NodeID != node.ID {
			continue
		}
		switch b.Job {
		case "work_queue":
			hasQueue = true
		case "timeline", "hot_cache":
			hasDataJob = true
		}
	}
	if hasQueue && !hasDataJob {
		return "noeviction"
	}
	return canvas
}

func jobServiceHost(bound DeploymentNode, design DeploymentDesign) string {
	switch bound.Type {
	case "cache":
		return redisServiceHost(bound.ID, design)
	case "db", "mq":
		return serviceHostForNodeType(bound.Type)
	default:
		return ""
	}
}

func cacheNodeCount(design DeploymentDesign) int {
	n := 0
	for _, node := range design.Nodes {
		if node.Type == "cache" {
			n++
		}
	}
	return n
}

func allowsSplitQueueCaches(design DeploymentDesign) bool {
	_, _, ok := splitQueueCache(design)
	return ok
}
