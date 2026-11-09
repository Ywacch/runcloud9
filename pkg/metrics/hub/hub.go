package hub

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

const (
	tickInterval     = 5 * time.Second
	queryTimeout     = 4 * time.Second
	maxQueryParallel = 8
	staleGrace       = 15 * time.Second
	metricsAPIPeriod = 15 * time.Second
)

// Querier runs one instant PromQL query and returns labeled samples.
type Querier interface {
	Query(ctx context.Context, promql string) ([]Sample, error)
}

// Sample is one vector element from Prometheus.
type Sample struct {
	Namespace string
	App       string
	Pod       string
	Le        string
	Value     float64
}

// AllPodMetricsLister lists pod metrics for the whole cluster (metrics.k8s.io).
type AllPodMetricsLister interface {
	ListAllPodMetrics(ctx context.Context) (*metricsv1beta1.PodMetricsList, error)
}

// PromQueryCounter records one Prometheus instant query (path hub, caller hub).
type PromQueryCounter func(path, caller, template string)

// ObserveHub counts hub control-plane metrics (refresh histogram, per-signal counters).
type ObserveHub interface {
	ObservePromQuery(path, caller, template string)
	ObserveRefresh(seconds float64)
	IncSignalError(signal string)
	IncSignalAbsent(signal string)
	IncUnlabeledSeries(signal string)
}

// Hub refreshes cluster-wide signal snapshots on a fixed tick.
type Hub struct {
	q         Querier
	catalog   []signals.Signal
	now       func() time.Time
	metrics   ObserveHub
	podLister AllPodMetricsLister

	snap atomic.Pointer[Snapshot]

	lastOK   map[signals.ID]time.Time
	lastData map[signals.ID]signalPayload

	mu           sync.Mutex
	lastPodFetch time.Time
	wg           sync.WaitGroup
}

type signalPayload struct {
	perNS  map[string]float64
	perApp map[string]map[string]float64
	perPod map[string]map[string]float64
	perLe  map[string]map[string]map[string]float64
}

// New builds a hub. catalog is typically signals.Catalog().
func New(q Querier, catalog []signals.Signal, now func() time.Time, obs ObserveHub, lister AllPodMetricsLister) *Hub {
	if now == nil {
		now = time.Now
	}
	h := &Hub{
		q:         q,
		catalog:   catalog,
		now:       now,
		metrics:   obs,
		podLister: lister,
		lastOK:    make(map[signals.ID]time.Time),
		lastData:  make(map[signals.ID]signalPayload),
	}
	empty := newSnapshot(now())
	h.snap.Store(empty)
	return h
}

// Snapshot returns the latest immutable snapshot pointer.
func (h *Hub) Snapshot() *Snapshot {
	if h == nil {
		return nil
	}
	return h.snap.Load()
}

// Start runs the refresh loop until ctx is done. WaitGroup is accounted in Start.
func (h *Hub) Start(ctx context.Context) {
	if h == nil || h.q == nil {
		return
	}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		h.refresh(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.refresh(ctx)
			}
		}
	}()
}

// Wait blocks until Start's goroutine exits (after ctx cancel).
func (h *Hub) Wait() {
	if h == nil {
		return
	}
	h.wg.Wait()
}

// RefreshOnce runs a single refresh tick (tests and manual checks).
func (h *Hub) RefreshOnce(ctx context.Context) {
	if h == nil {
		return
	}
	h.refresh(ctx)
}

func (h *Hub) refresh(ctx context.Context) {
	start := h.now()
	next := newSnapshot(start)

	sem := make(chan struct{}, maxQueryParallel)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, sig := range h.catalog {
		wg.Add(1)
		go func(s signals.Signal) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			h.refreshSignal(ctx, s, next, &mu)
		}(sig)
	}
	wg.Wait()

	h.refreshMetricsAPI(ctx, next, start)

	h.mu.Lock()
	prev := h.snap.Load()
	h.mergeStale(prev, next, start)
	h.snap.Store(next)
	h.mu.Unlock()

	if h.metrics != nil {
		h.metrics.ObserveRefresh(h.now().Sub(start).Seconds())
	}
}

func (h *Hub) refreshSignal(ctx context.Context, sig signals.Signal, snap *Snapshot, mu *sync.Mutex) {
	promql, err := signals.RenderCluster(sig)
	if err != nil {
		h.noteSignalErr(sig.ID, snap, err)
		return
	}
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	if h.metrics != nil {
		h.metrics.ObservePromQuery("hub", "hub", "none")
	}
	samples, err := h.q.Query(qctx, promql)
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		h.applyFailedSignal(sig, snap, err)
		return
	}
	if len(samples) == 0 {
		if h.metrics != nil {
			h.metrics.IncSignalAbsent(string(sig.ID))
		}
		h.applyAbsentOrStale(sig, snap)
		return
	}
	payload := ingestSamples(sig, samples, h.metrics, string(sig.ID))
	h.applyPayload(sig, snap, payload, false, nil)
	h.mu.Lock()
	h.lastOK[sig.ID] = snap.At
	h.lastData[sig.ID] = payload
	h.mu.Unlock()
}

func (h *Hub) applyFailedSignal(sig signals.Signal, snap *Snapshot, err error) {
	if h.metrics != nil {
		h.metrics.IncSignalError(string(sig.ID))
	}
	h.mu.Lock()
	last, ok := h.lastOK[sig.ID]
	payload, have := h.lastData[sig.ID]
	h.mu.Unlock()
	if ok && snap.At.Sub(last) <= staleGrace && have {
		h.applyPayload(sig, snap, payload, true, err)
		return
	}
	snap.meta[sig.ID] = signalMeta{FetchedAt: snap.At, Err: err != nil, Absent: true}
}

func (h *Hub) applyAbsentOrStale(sig signals.Signal, snap *Snapshot) {
	h.mu.Lock()
	last, ok := h.lastOK[sig.ID]
	payload, have := h.lastData[sig.ID]
	h.mu.Unlock()
	if ok && snap.At.Sub(last) <= staleGrace && have {
		h.applyPayload(sig, snap, payload, true, nil)
		return
	}
	snap.meta[sig.ID] = signalMeta{FetchedAt: snap.At, Absent: true}
}

func (h *Hub) applyPayload(sig signals.Signal, snap *Snapshot, payload signalPayload, stale bool, err error) {
	snap.meta[sig.ID] = signalMeta{
		FetchedAt: snap.At,
		Err:       err != nil,
		Stale:     stale,
		Absent:    false,
	}
	if len(payload.perNS) > 0 {
		snap.perNS[sig.ID] = cloneNS(payload.perNS)
	}
	if len(payload.perApp) > 0 {
		snap.perApp[sig.ID] = cloneApp(payload.perApp)
	}
	if len(payload.perPod) > 0 {
		snap.perPod[sig.ID] = cloneApp(payload.perPod)
	}
	if len(payload.perLe) > 0 {
		snap.perLe[sig.ID] = cloneLe(payload.perLe)
	}
}

func ingestSamples(sig signals.Signal, samples []Sample, obs ObserveHub, signal string) signalPayload {
	var out signalPayload
	for _, sm := range samples {
		if sm.Namespace == "" {
			if obs != nil {
				obs.IncUnlabeledSeries(signal)
			}
			continue
		}
		switch sig.Shape {
		case signals.PerNamespace:
			if out.perNS == nil {
				out.perNS = make(map[string]float64)
			}
			out.perNS[sm.Namespace] += sm.Value
		case signals.PerApp:
			if out.perApp == nil {
				out.perApp = make(map[string]map[string]float64)
			}
			if out.perApp[sm.Namespace] == nil {
				out.perApp[sm.Namespace] = make(map[string]float64)
			}
			app := sm.App
			if app == "" {
				app = "unknown"
			}
			out.perApp[sm.Namespace][app] = sm.Value
		case signals.PerPod:
			if out.perPod == nil {
				out.perPod = make(map[string]map[string]float64)
			}
			if out.perPod[sm.Namespace] == nil {
				out.perPod[sm.Namespace] = make(map[string]float64)
			}
			pod := sm.Pod
			if pod == "" {
				continue
			}
			out.perPod[sm.Namespace][pod] = sm.Value
		case signals.PerBucket:
			if out.perLe == nil {
				out.perLe = make(map[string]map[string]map[string]float64)
			}
			if out.perLe[sm.Namespace] == nil {
				out.perLe[sm.Namespace] = make(map[string]map[string]float64)
			}
			app := sm.App
			if app == "" {
				app = "unknown"
			}
			if out.perLe[sm.Namespace][app] == nil {
				out.perLe[sm.Namespace][app] = make(map[string]float64)
			}
			if sm.Le != "" {
				out.perLe[sm.Namespace][app][sm.Le] = sm.Value
			}
		}
	}
	return out
}

func (h *Hub) refreshMetricsAPI(ctx context.Context, snap *Snapshot, at time.Time) {
	if h.podLister == nil {
		return
	}
	h.mu.Lock()
	need := h.lastPodFetch.IsZero() || at.Sub(h.lastPodFetch) >= metricsAPIPeriod
	h.mu.Unlock()
	if !need {
		if prev := h.snap.Load(); prev != nil {
			snap.podUsage = prev.podUsage
		}
		return
	}
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	list, err := h.podLister.ListAllPodMetrics(qctx)
	if err != nil {
		return
	}
	byNS := make(map[string]map[string]map[string]containerUsage)
	for i := range list.Items {
		pm := &list.Items[i]
		ns := pm.Namespace
		if byNS[ns] == nil {
			byNS[ns] = make(map[string]map[string]containerUsage)
		}
		podName := pm.Name
		if byNS[ns][podName] == nil {
			byNS[ns][podName] = make(map[string]containerUsage)
		}
		for _, c := range pm.Containers {
			cpu := c.Usage.Cpu().MilliValue()
			mem := c.Usage.Memory().Value()
			byNS[ns][podName][c.Name] = containerUsage{CPUMilli: cpu, MemBytes: mem}
		}
	}
	snap.podUsage = metricsAPIState{At: at, ByPod: byNS}
	h.mu.Lock()
	h.lastPodFetch = at
	h.mu.Unlock()
}

func (h *Hub) mergeStale(prev, next *Snapshot, at time.Time) {
	if prev == nil {
		return
	}
	for id, meta := range prev.meta {
		if _, ok := next.meta[id]; ok {
			continue
		}
		if at.Sub(meta.FetchedAt) > staleGrace {
			continue
		}
		next.meta[id] = signalMeta{FetchedAt: meta.FetchedAt, Stale: true}
		if m, ok := prev.perNS[id]; ok {
			next.perNS[id] = cloneNS(m)
		}
		if m, ok := prev.perApp[id]; ok {
			next.perApp[id] = cloneApp(m)
		}
		if m, ok := prev.perPod[id]; ok {
			next.perPod[id] = cloneApp(m)
		}
		if m, ok := prev.perLe[id]; ok {
			next.perLe[id] = cloneLe(m)
		}
	}
}

func (h *Hub) noteSignalErr(id signals.ID, snap *Snapshot, err error) {
	if h.metrics != nil {
		h.metrics.IncSignalError(string(id))
	}
	h.applyFailedSignal(signals.Signal{ID: id}, snap, err)
}

func cloneNS(in map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneApp(in map[string]map[string]float64) map[string]map[string]float64 {
	out := make(map[string]map[string]float64, len(in))
	for ns, apps := range in {
		inner := make(map[string]float64, len(apps))
		for a, v := range apps {
			inner[a] = v
		}
		out[ns] = inner
	}
	return out
}

func cloneLe(in map[string]map[string]map[string]float64) map[string]map[string]map[string]float64 {
	out := make(map[string]map[string]map[string]float64, len(in))
	for ns, apps := range in {
		inner := make(map[string]map[string]float64, len(apps))
		for app, lem := range apps {
			buckets := make(map[string]float64, len(lem))
			for le, v := range lem {
				buckets[le] = v
			}
			inner[app] = buckets
		}
		out[ns] = inner
	}
	return out
}
