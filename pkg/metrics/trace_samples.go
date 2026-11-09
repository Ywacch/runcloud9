package metrics

import (
	"context"
	"encoding/json"

	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

const (
	traceSampleScrapeEveryCollect = 3
	traceSamplesSummaryCap        = 20
)

type traceSampleAccumulator struct {
	order []string
	byID  map[string]trace.JourneySample
}

func newTraceSampleAccumulator() *traceSampleAccumulator {
	return &traceSampleAccumulator{byID: make(map[string]trace.JourneySample)}
}

func (a *traceSampleAccumulator) merge(samples []trace.JourneySample) {
	if a == nil || len(samples) == 0 {
		return
	}
	if a.byID == nil {
		a.byID = make(map[string]trace.JourneySample)
	}
	for _, s := range samples {
		id := s.RequestID
		if id == "" {
			continue
		}
		if _, seen := a.byID[id]; !seen {
			a.order = append(a.order, id)
		}
		a.byID[id] = s
	}
	for len(a.order) > traceSamplesSummaryCap {
		oldest := a.order[0]
		a.order = a.order[1:]
		delete(a.byID, oldest)
	}
}

func (a *traceSampleAccumulator) snapshot() []trace.JourneySample {
	if a == nil || len(a.byID) == 0 {
		return nil
	}
	out := make([]trace.JourneySample, 0, len(a.order))
	for i := len(a.order) - 1; i >= 0; i-- {
		id := a.order[i]
		if s, ok := a.byID[id]; ok {
			out = append(out, s)
		}
	}
	return out
}

func (c *MetricsCollector) maybeScrapeTraceSamples(ctx context.Context, namespace string) {
	if c == nil || namespace == "" {
		return
	}
	c.traceSampleMu.Lock()
	if c.traceCollectTick == nil {
		c.traceCollectTick = make(map[string]int)
	}
	c.traceCollectTick[namespace]++
	tick := c.traceCollectTick[namespace]
	c.traceSampleMu.Unlock()
	if tick%traceSampleScrapeEveryCollect != 0 {
		return
	}
	release, ok := c.beginSideWorkScrape(ctx, namespace)
	if !ok {
		return
	}
	defer release()
	c.mergeTraceSamples(namespace, c.scrapeTraceSamples(ctx, namespace))
}

func (c *MetricsCollector) mergeTraceSamples(namespace string, samples []trace.JourneySample) {
	if c == nil || namespace == "" || len(samples) == 0 {
		return
	}
	c.traceSampleMu.Lock()
	defer c.traceSampleMu.Unlock()
	if c.traceSamples == nil {
		c.traceSamples = make(map[string]*traceSampleAccumulator)
	}
	acc := c.traceSamples[namespace]
	if acc == nil {
		acc = newTraceSampleAccumulator()
		c.traceSamples[namespace] = acc
	}
	acc.merge(samples)
}

// TraceSamplesFor returns persisted journey samples (newest first) for metrics_summary.
func (c *MetricsCollector) TraceSamplesFor(namespace string) []trace.JourneySample {
	if c == nil || namespace == "" {
		return nil
	}
	c.traceSampleMu.Lock()
	defer c.traceSampleMu.Unlock()
	acc := c.traceSamples[namespace]
	if acc == nil {
		return nil
	}
	return acc.snapshot()
}

// TraceSamplesFromSummary decodes metrics_summary.trace_samples.
func TraceSamplesFromSummary(summary map[string]any) ([]trace.JourneySample, error) {
	if summary == nil {
		return nil, nil
	}
	raw, ok := summary["trace_samples"]
	if !ok || raw == nil {
		return nil, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var out []trace.JourneySample
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}
