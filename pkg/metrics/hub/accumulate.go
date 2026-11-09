package hub

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

// rateWindow matches pkg/metrics.PromRateWindow; duplicated to avoid an import cycle.
const rateWindow = "[10s]"

// StreamKey names one integrated session series per namespace.
type StreamKey string

const (
	StreamHTTPRequests      StreamKey = "http_requests"
	StreamHTTP5xx           StreamKey = "http_5xx"
	StreamCacheHits         StreamKey = "cache_hits"
	StreamCacheMisses       StreamKey = "cache_misses"
	StreamCacheErrors       StreamKey = "cache_errors"
	StreamSQLQueries        StreamKey = "sql_queries"
	StreamWorkerProcessed   StreamKey = "worker_processed"
	StreamWorkerErrors      StreamKey = "worker_errors"
	StreamAppHTTPLatency    StreamKey = "app_http_latency"
	StreamWorkerProcLatency StreamKey = "worker_proc_latency"
)

type nsWindow struct {
	startedAt time.Time
	lastAt    map[StreamKey]time.Time
	totals    map[StreamKey]float64
	buckets   map[StreamKey]map[string]float64
}

// WindowAccumulator integrates per-namespace rates on each hub tick.
type WindowAccumulator struct {
	mu  sync.Mutex
	now func() time.Time
	ns  map[string]*nsWindow
}

// NewWindowAccumulator builds empty session-window state.
func NewWindowAccumulator(now func() time.Time) *WindowAccumulator {
	if now == nil {
		now = time.Now
	}
	return &WindowAccumulator{
		now: now,
		ns:  make(map[string]*nsWindow),
	}
}

// StartedAt is when integration began for a namespace (first successful sample).
func (a *WindowAccumulator) StartedAt(namespace string) (time.Time, bool) {
	if a == nil || namespace == "" {
		return time.Time{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.ns[namespace]
	if !ok || st.startedAt.IsZero() {
		return time.Time{}, false
	}
	return st.startedAt, true
}

// Total returns the integrated total for one stream.
func (a *WindowAccumulator) Total(namespace string, key StreamKey) (float64, bool) {
	if a == nil || namespace == "" {
		return 0, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.ns[namespace]
	if !ok {
		return 0, false
	}
	if !windowReady(st, key) {
		return 0, false
	}
	return st.totals[key], true
}

// Quantile estimates a quantile from integrated histogram buckets (Prometheus interpolation).
func (a *WindowAccumulator) Quantile(namespace string, key StreamKey, q float64) (float64, bool) {
	if a == nil || namespace == "" {
		return 0, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.ns[namespace]
	if !ok {
		return 0, false
	}
	if !windowReady(st, key) {
		return 0, false
	}
	buckets, ok := st.buckets[key]
	if !ok || len(buckets) == 0 {
		return 0, false
	}
	v, ok := histogramQuantile(q, buckets)
	return v, ok
}

func windowReady(st *nsWindow, key StreamKey) bool {
	if st == nil || st.startedAt.IsZero() {
		return false
	}
	last := st.lastAt[key]
	return !last.IsZero() && last.After(st.startedAt)
}

func (a *WindowAccumulator) integrateRate(namespace string, at time.Time, key StreamKey, rate float64, ok bool) {
	if a == nil || namespace == "" || !ok || rate < 0 || math.IsNaN(rate) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	st := a.ensureNS(namespace)
	if st.startedAt.IsZero() {
		st.startedAt = at
	}
	if prev, has := st.lastAt[key]; has && at.After(prev) {
		dt := at.Sub(prev).Seconds()
		if dt > 0 {
			st.totals[key] += rate * dt
		}
	}
	st.lastAt[key] = at
}

func (a *WindowAccumulator) integrateBuckets(namespace string, at time.Time, key StreamKey, rates map[string]float64) {
	if a == nil || namespace == "" || len(rates) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	st := a.ensureNS(namespace)
	if st.startedAt.IsZero() {
		st.startedAt = at
	}
	if st.buckets[key] == nil {
		st.buckets[key] = make(map[string]float64)
	}
	prev := st.lastAt[key]
	var dt float64
	if !prev.IsZero() && at.After(prev) {
		dt = at.Sub(prev).Seconds()
	}
	for le, rate := range rates {
		if rate < 0 || math.IsNaN(rate) {
			continue
		}
		if dt > 0 {
			st.buckets[key][le] += rate * dt
		}
	}
	st.lastAt[key] = at
}

func (a *WindowAccumulator) ensureNS(namespace string) *nsWindow {
	st, ok := a.ns[namespace]
	if !ok {
		st = &nsWindow{
			lastAt:  make(map[StreamKey]time.Time),
			totals:  make(map[StreamKey]float64),
			buckets: make(map[StreamKey]map[string]float64),
		}
		a.ns[namespace] = st
	}
	return st
}

func appServerSum(apps map[string]float64) (float64, bool) {
	if len(apps) == 0 {
		return 0, false
	}
	if v, ok := apps["app-server"]; ok {
		return v, true
	}
	var sum float64
	for _, v := range apps {
		sum += v
	}
	return sum, true
}

func sumPods(pods map[string]float64) (float64, bool) {
	if len(pods) == 0 {
		return 0, false
	}
	var sum float64
	for _, v := range pods {
		sum += v
	}
	return sum, true
}

func (h *Hub) refreshSessionWindows(ctx context.Context, snap *Snapshot) {
	if h == nil || h.window == nil || snap == nil {
		return
	}
	at := snap.At
	seen := make(map[string]struct{})
	collectNS := func(ns string) {
		if ns != "" {
			seen[ns] = struct{}{}
		}
	}
	for ns := range snap.perNS[signals.AppRequestRate] {
		collectNS(ns)
	}
	for ns := range snap.perApp[signals.AppRequestRate] {
		collectNS(ns)
	}
	for ns := range snap.perPod[signals.WorkerProcessedRatePod] {
		collectNS(ns)
	}
	for _, ns := range namespaceSet(seen) {
		if apps, ok, _ := snap.ByApp(ns, signals.AppRequestRate); ok {
			if v, ok := appServerSum(apps); ok {
				h.window.integrateRate(ns, at, StreamHTTPRequests, v, true)
			}
		}
		if apps, ok, _ := snap.ByApp(ns, signals.AppErrorRate); ok {
			if v, ok := appServerSum(apps); ok {
				h.window.integrateRate(ns, at, StreamHTTP5xx, v, true)
			}
		}
		if apps, ok, _ := snap.ByApp(ns, signals.AppCacheHitsRate); ok {
			if v, ok := appServerSum(apps); ok {
				h.window.integrateRate(ns, at, StreamCacheHits, v, true)
			}
		}
		if apps, ok, _ := snap.ByApp(ns, signals.AppCacheMissesRate); ok {
			if v, ok := appServerSum(apps); ok {
				h.window.integrateRate(ns, at, StreamCacheMisses, v, true)
			}
		}
		if apps, ok, _ := snap.ByApp(ns, signals.AppCacheErrorsRate); ok {
			if v, ok := appServerSum(apps); ok {
				h.window.integrateRate(ns, at, StreamCacheErrors, v, true)
			}
		}
		if pods, ok, _ := snap.ByPod(ns, signals.WorkerProcessedRatePod); ok {
			if v, ok := sumPods(pods); ok {
				h.window.integrateRate(ns, at, StreamWorkerProcessed, v, true)
			}
		}
		if pods, ok, _ := snap.ByPod(ns, signals.WorkerErrorsRatePod); ok {
			if v, ok := sumPods(pods); ok {
				h.window.integrateRate(ns, at, StreamWorkerErrors, v, true)
			}
		}
	}
	if h.q == nil {
		return
	}
	queries := []struct {
		key StreamKey
		ql  string
	}{
		{StreamSQLQueries, fmt.Sprintf(`sum by (namespace) (rate(db_query_duration_seconds_count{deployment!=""}%s))`, rateWindow)},
		{StreamAppHTTPLatency, fmt.Sprintf(`sum by (namespace, le) (rate(http_request_duration_seconds_bucket{deployment!=""}%s))`, rateWindow)},
		{StreamWorkerProcLatency, fmt.Sprintf(`sum by (namespace, le) (rate({__name__=~".*_worker_processing_duration_seconds_bucket",deployment!=""}%s))`, rateWindow)},
	}
	for _, item := range queries {
		qctx, cancel := context.WithTimeout(ctx, queryTimeout)
		samples, err := h.q.Query(qctx, item.ql)
		cancel()
		if err != nil || len(samples) == 0 {
			continue
		}
		if h.metrics != nil {
			h.metrics.ObservePromQuery("hub", "hub", "none")
		}
		switch item.key {
		case StreamSQLQueries:
			for _, sm := range samples {
				h.window.integrateRate(sm.Namespace, at, StreamSQLQueries, sm.Value, sm.Namespace != "")
			}
		default:
			byNS := groupBucketRates(samples)
			for ns, rates := range byNS {
				h.window.integrateBuckets(ns, at, item.key, rates)
			}
		}
	}
}

func namespaceSet(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for ns := range m {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

func groupBucketRates(samples []Sample) map[string]map[string]float64 {
	out := make(map[string]map[string]float64)
	for _, sm := range samples {
		if sm.Namespace == "" || sm.Le == "" {
			continue
		}
		if out[sm.Namespace] == nil {
			out[sm.Namespace] = make(map[string]float64)
		}
		out[sm.Namespace][sm.Le] += sm.Value
	}
	return out
}

// histogramQuantile implements Prometheus histogram_quantile.
// Bucket counts are cumulative: the +Inf count is the observation total.
func histogramQuantile(q float64, buckets map[string]float64) (float64, bool) {
	if q < 0 || q > 1 || len(buckets) < 2 {
		return 0, false
	}
	type bc struct {
		upper float64
		count float64
	}
	list := make([]bc, 0, len(buckets))
	for leStr, count := range buckets {
		if count < 0 || math.IsNaN(count) {
			continue
		}
		upper, ok := bucketUpperBound(leStr)
		if !ok {
			continue
		}
		list = append(list, bc{upper: upper, count: count})
	}
	if len(list) < 2 {
		return 0, false
	}
	sort.Slice(list, func(i, j int) bool { return list[i].upper < list[j].upper })
	for i := 1; i < len(list); i++ {
		if list[i].count < list[i-1].count {
			list[i].count = list[i-1].count
		}
	}
	total := list[len(list)-1].count
	if total == 0 {
		return 0, false
	}
	rank := q * total
	b := 0
	for b < len(list)-1 && list[b].count < rank {
		b++
	}
	if b == len(list)-1 {
		return list[len(list)-2].upper, true
	}
	bucketEnd := list[b].upper
	count := list[b].count
	bucketStart := 0.0
	if b > 0 {
		bucketStart = list[b-1].upper
		count -= list[b-1].count
		rank -= list[b-1].count
	}
	if count == 0 {
		return bucketStart, true
	}
	return bucketStart + (bucketEnd-bucketStart)*(rank/count), true
}

func bucketUpperBound(le string) (float64, bool) {
	if le == "+Inf" {
		return math.Inf(1), true
	}
	v, err := strconv.ParseFloat(le, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
