package metrics

// ComponentInternals maps canvas roles to per-instance metric summaries for brain
// context. Values are point-in-time summaries, not time series.
type ComponentInternals map[string]ComponentInternal

// ComponentInternal groups one or more instances under a canvas role.
type ComponentInternal struct {
	Instances []InstanceInternal `json:"instances"`
}

// InstanceInternal is one process or replica layer with normalized signals,
// engine-specific extras, and per-signal freshness tags.
type InstanceInternal struct {
	ID           string                     `json:"id"`
	Type         string                     `json:"type,omitempty"`
	Engine       string                     `json:"engine,omitempty"`
	Signals      map[string]any             `json:"signals"`
	EngineExtras map[string]any             `json:"engineExtras,omitempty"`
	Freshness    map[string]SignalFreshness `json:"freshness,omitempty"`
}

// BuildComponentInternals projects the final metrics snapshot into a stable
// per-role, per-instance summary. No new PromQL is issued.
func BuildComponentInternals(dm *DeploymentMetrics) ComponentInternals {
	if dm == nil || len(dm.Components) == 0 {
		return ComponentInternals{}
	}
	annotateFreshness(dm)
	out := make(ComponentInternals, len(dm.Components))
	for role, cm := range dm.Components {
		instances := instancesFromComponent(role, cm)
		if len(instances) == 0 {
			continue
		}
		out[role] = ComponentInternal{Instances: instances}
	}
	return out
}

func instancesFromComponent(role string, cm ComponentMetrics) []InstanceInternal {
	if len(cm.Layers) > 0 {
		clusterExtras := componentEngineExtras(cm)
		instances := make([]InstanceInternal, 0, len(cm.Layers))
		for _, layer := range cm.Layers {
			inst := instanceFromLayer(role, layer)
			if layer.Role == dbLayerRolePrimary || layer.ID == dbLayerIDPostgresPrimary {
				mergeEngineExtras(&inst, clusterExtras)
				mergeFreshness(&inst, cm.Freshness)
			}
			instances = append(instances, inst)
		}
		return instances
	}
	if len(cm.Replicas) > 0 {
		instances := make([]InstanceInternal, 0, len(cm.Replicas))
		for _, rep := range cm.Replicas {
			instances = append(instances, instanceFromPod(role, rep.Name, rep))
		}
		return instances
	}
	if len(cm.BackendPods) > 0 {
		instances := make([]InstanceInternal, 0, len(cm.BackendPods))
		for _, pod := range cm.BackendPods {
			instances = append(instances, instanceFromPod(role, pod.Name, pod))
		}
		return instances
	}
	return []InstanceInternal{instanceFromComponentMetrics(role, role, cm)}
}

func instanceFromComponentMetrics(role, id string, cm ComponentMetrics) InstanceInternal {
	inst := InstanceInternal{
		ID:           id,
		Type:         role,
		Signals:      componentSignals(cm),
		EngineExtras: componentEngineExtras(cm),
	}
	if cm.StorageEngine != "" {
		inst.Engine = cm.StorageEngine
	}
	if len(cm.Freshness) > 0 {
		inst.Freshness = cm.Freshness
	}
	return inst
}

func instanceFromLayer(role string, layer ComponentLayerMetrics) InstanceInternal {
	inst := InstanceInternal{
		ID:           layer.ID,
		Type:         layer.Role,
		Engine:       layer.Engine,
		Signals:      layerSignals(layer),
		EngineExtras: layerEngineExtras(layer),
	}
	if len(layer.Freshness) > 0 {
		inst.Freshness = layer.Freshness
	}
	if inst.Type == "" {
		inst.Type = role
	}
	return inst
}

func instanceFromPod(role, id string, pod PodMetrics) InstanceInternal {
	return InstanceInternal{
		ID:      id,
		Type:    role,
		Signals: podSignals(pod),
	}
}

func componentSignals(cm ComponentMetrics) map[string]any {
	m := make(map[string]any)
	setFloat(m, "cpuUsage", cm.CPUUsage)
	setInt64(m, "memoryUsage", cm.MemoryBytes)
	setFloat(m, "requestRate", cm.RequestRate)
	setFloat(m, "totalRequests", cm.TotalRequests)
	setFloat(m, "p50Latency", cm.P50Latency)
	setFloat(m, "p95Latency", cm.P95Latency)
	setFloat(m, "p99Latency", cm.P99Latency)
	setFloat(m, "avgLatency", cm.AvgLatency)
	setFloat(m, "cacheHits", cm.CacheHits)
	setFloat(m, "cacheMisses", cm.CacheMisses)
	setFloat(m, "cacheErrors", cm.CacheErrors)
	setFloat(m, "cacheHitsLast9m", cm.CacheHitsLast9m)
	setFloat(m, "cacheMissesLast9m", cm.CacheMissesLast9m)
	setFloat(m, "cacheErrorsLast9m", cm.CacheErrorsLast9m)
	setFloat(m, "errorRate", cm.ErrorRate)
	setFloat(m, "totalErrors9m", cm.TotalErrors9m)
	appendRedisCommandSignals(m, cm)
	setFloat(m, "p50Latency9m", cm.P50Latency9m)
	setFloat(m, "p95Latency9m", cm.P95Latency9m)
	setFloat(m, "p99Latency9m", cm.P99Latency9m)
	setFloat(m, "dbQueryRate", cm.DbQueryRate)
	setFloat(m, "dbQueryLatency", cm.DbQueryLatency)
	setFloat(m, "dbTotalQueries", cm.DbTotalQueries)
	setFloat(m, "dbActiveConnections", cm.DbActiveConnections)
	setFloat(m, "maxConnections", cm.MaxConnections)
	setFloat(m, "evictionsPerSec", cm.EvictionsPerSec)
	setFloat(m, "expirationsPerSec", cm.ExpirationsPerSec)
	setFloat(m, "evictionsLast9m", cm.EvictionsLast9m)
	setFloat(m, "expirationsLast9m", cm.ExpirationsLast9m)
	setFloatPtr(m, "timelineRelationBytes", cm.TimelineRelationBytes)
	setFloatPtr(m, "timelineDeadTuples", cm.TimelineDeadTuples)
	setFloat(m, "activeConnections", cm.ActiveConnections)
	setFloat(m, "queueDepth", cm.QueueDepth)
	setFloat(m, "produceRate", cm.ProduceRate)
	setFloat(m, "consumeRate", cm.ConsumeRate)
	setFloat(m, "messagesReady", cm.MessagesReady)
	setFloat(m, "messagesUnacked", cm.MessagesUnacked)
	setFloat(m, "mqSaturationRatio", cm.MQSaturationRatio)
	setFloat(m, "workerSaturation", cm.WorkerSaturation)
	setFloat(m, "cpuLimit", cm.CPULimit)
	setInt64(m, "memoryLimit", cm.MemoryLimit)
	return m
}

func componentEngineExtras(cm ComponentMetrics) map[string]any {
	m := make(map[string]any)
	if cm.StorageIO != nil {
		m["storageIo"] = cm.StorageIO
	}
	if cm.Consistency != nil {
		m["consistency"] = cm.Consistency
	}
	if cm.RuntimeHealth != nil {
		m["runtimeHealth"] = cm.RuntimeHealth
	}
	setString(m, "storageEngine", cm.StorageEngine)
	setString(m, "storageRole", cm.StorageRole)
	setString(m, "cacheStrategy", cm.CacheStrategy)
	setString(m, "cacheMetricMode", cm.CacheMetricMode)
	setString(m, "cacheReadOperation", cm.CacheReadOperation)
	setString(m, "cacheWriteOperation", cm.CacheWriteOperation)
	setString(m, "dbQuerySource", cm.DbQuerySource)
	setString(m, "cpuSource", cm.CPUSource)
	setString(m, "memSource", cm.MemSource)
	setFloat(m, "redisKeyspaceHits", cm.RedisKeyspaceHits)
	setFloat(m, "redisKeyspaceMisses", cm.RedisKeyspaceMisses)
	setFloat(m, "redisOpsPerSec", cm.RedisOpsPerSec)
	setFloat(m, "redisRejectedRate", cm.RedisRejectedRate)
	setFloat(m, "redisMaxClients", cm.RedisMaxClients)
	setString(m, "lbAlgorithm", cm.LBAlgorithm)
	setFloat(m, "lbConnectionLimit", cm.LBConnectionLimit)
	setFloat(m, "mqMemoryUsed", cm.MQMemoryUsed)
	setFloat(m, "mqMemoryLimit", cm.MQMemoryLimit)
	if cm.CPUIsEstimated {
		m["cpuIsEstimated"] = true
	}
	if cm.MemIsEstimated {
		m["memIsEstimated"] = true
	}
	if len(cm.LBBackends) > 0 {
		m["lbBackends"] = cm.LBBackends
	}
	if len(cm.PerBackendRPS) > 0 {
		m["perBackendRps"] = cm.PerBackendRPS
	}
	if cm.BackendReplicas != nil {
		m["backendReplicas"] = cm.BackendReplicas
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func layerSignals(layer ComponentLayerMetrics) map[string]any {
	m := make(map[string]any)
	setFloat(m, "cpuUsage", layer.CPUUsage)
	setInt64(m, "memoryUsage", layer.MemoryBytes)
	setFloat(m, "requestRate", layer.RequestRate)
	setFloat(m, "totalRequests", layer.TotalRequests)
	setFloat(m, "p50Latency", layer.P50Latency)
	setFloat(m, "p95Latency", layer.P95Latency)
	setFloat(m, "p99Latency", layer.P99Latency)
	setFloat(m, "p50Latency9m", layer.P50Latency9m)
	setFloat(m, "p95Latency9m", layer.P95Latency9m)
	setFloat(m, "p99Latency9m", layer.P99Latency9m)
	setFloat(m, "dbQueryRate", layer.DbQueryRate)
	setFloat(m, "dbTotalQueries", layer.DbTotalQueries)
	setFloat(m, "dbActiveConnections", layer.DbActiveConnections)
	appendRedisCommandSignalsToLayer(m, layer)
	setFloat(m, "redisKeyspaceHits", layer.RedisKeyspaceHits)
	setFloat(m, "redisKeyspaceMisses", layer.RedisKeyspaceMisses)
	setFloat(m, "redisRejectedRate", layer.RedisRejectedRate)
	setFloat(m, "evictionsPerSec", layer.EvictionsPerSec)
	setFloat(m, "expirationsPerSec", layer.ExpirationsPerSec)
	setFloat(m, "evictionsLast9m", layer.EvictionsLast9m)
	setFloat(m, "expirationsLast9m", layer.ExpirationsLast9m)
	setInt32(m, "restartCount", layer.RestartCount)
	setFloat(m, "cpuLimit", layer.CPULimit)
	setInt64(m, "memoryLimit", layer.MemoryLimit)
	if layer.ReplicationLagSeconds != nil {
		m["replicationLagSeconds"] = *layer.ReplicationLagSeconds
	}
	if layer.LagBytes != nil {
		m["lagBytes"] = *layer.LagBytes
	}
	return m
}

func layerEngineExtras(layer ComponentLayerMetrics) map[string]any {
	m := make(map[string]any)
	if layer.StorageIO != nil {
		m["storageIo"] = layer.StorageIO
	}
	setString(m, "syncState", layer.SyncState)
	if layer.PromotionCandidate {
		m["promotionCandidate"] = true
	}
	if layer.ConnectedOnPrimary {
		m["connectedOnPrimary"] = true
	} else if layer.Role == dbLayerRoleReplica {
		m["connectedOnPrimary"] = false
	}
	setString(m, "cpuSource", layer.CPUSource)
	setString(m, "memSource", layer.MemSource)
	if layer.CPUIsEstimated {
		m["cpuIsEstimated"] = true
	}
	if layer.MemIsEstimated {
		m["memIsEstimated"] = true
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func podSignals(pod PodMetrics) map[string]any {
	m := make(map[string]any)
	setFloat(m, "requestRate", pod.RequestRate)
	setFloat(m, "totalRequests", pod.TotalRequests)
	setFloat(m, "p50Latency", pod.P50Latency)
	setFloat(m, "p95Latency", pod.P95Latency)
	setFloat(m, "p99Latency", pod.P99Latency)
	setFloat(m, "totalErrors9m", pod.TotalErrors9m)
	setFloat(m, "p50Latency9m", pod.P50Latency9m)
	setFloat(m, "p95Latency9m", pod.P95Latency9m)
	setFloat(m, "p99Latency9m", pod.P99Latency9m)
	setFloat(m, "cpuUsage", pod.CPUUsage)
	setInt64(m, "memoryUsage", pod.MemoryBytes)
	setFloat(m, "errorRate", pod.ErrorRate)
	if pod.Ready {
		m["ready"] = true
	}
	return m
}

func setFloat(m map[string]any, key string, v float64) {
	if v != 0 {
		m[key] = v
	}
}

func setFloatPtr(m map[string]any, key string, v *float64) {
	if v != nil {
		m[key] = *v
	}
}

func setInt64(m map[string]any, key string, v int64) {
	if v != 0 {
		m[key] = v
	}
}

func setInt32(m map[string]any, key string, v int32) {
	if v != 0 {
		m[key] = v
	}
}

func setString(m map[string]any, key, v string) {
	if v != "" {
		m[key] = v
	}
}

func mergeEngineExtras(inst *InstanceInternal, extras map[string]any) {
	if inst == nil || len(extras) == 0 {
		return
	}
	if inst.EngineExtras == nil {
		inst.EngineExtras = make(map[string]any, len(extras))
	}
	for k, v := range extras {
		if _, exists := inst.EngineExtras[k]; !exists {
			inst.EngineExtras[k] = v
		}
	}
}

func mergeFreshness(inst *InstanceInternal, fresh map[string]SignalFreshness) {
	if inst == nil || len(fresh) == 0 {
		return
	}
	if inst.Freshness == nil {
		inst.Freshness = make(map[string]SignalFreshness, len(fresh))
	}
	for k, v := range fresh {
		if _, exists := inst.Freshness[k]; !exists {
			inst.Freshness[k] = v
		}
	}
}
