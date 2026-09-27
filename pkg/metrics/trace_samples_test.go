package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

func TestTraceSampleAccumulator_DedupesByRequestID(t *testing.T) {
	t.Parallel()
	acc := newTraceSampleAccumulator()
	acc.merge([]trace.JourneySample{
		{RequestID: "a", Op: "/r", DurationMs: 1},
		{RequestID: "a", Op: "/r", DurationMs: 9, Spans: []trace.JourneySpan{{Component: "db", Op: "select", DurationMs: 2}}},
	})
	got := acc.snapshot()
	if len(got) != 1 {
		t.Fatalf("len: %d", len(got))
	}
	if got[0].DurationMs != 9 || len(got[0].Spans) != 1 {
		t.Fatalf("deduped sample: %+v", got[0])
	}
}

func TestTraceSampleAccumulator_CapsAtTwenty(t *testing.T) {
	t.Parallel()
	acc := newTraceSampleAccumulator()
	var batch []trace.JourneySample
	for i := 0; i < traceSamplesSummaryCap+5; i++ {
		batch = append(batch, trace.JourneySample{RequestID: fmt.Sprintf("req-%d", i), Op: "/r"})
	}
	acc.merge(batch)
	if len(acc.order) != traceSamplesSummaryCap {
		t.Fatalf("order len: %d want %d", len(acc.order), traceSamplesSummaryCap)
	}
	if len(acc.snapshot()) != traceSamplesSummaryCap {
		t.Fatalf("snapshot len: %d", len(acc.snapshot()))
	}
}

func TestParseTraceSamplesJSON(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string][]trace.JourneySample{
		"samples": {{RequestID: "rid-1", Op: "/feed", DurationMs: 4}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := parseTraceSamplesJSON(body)
	if len(got) != 1 || got[0].RequestID != "rid-1" {
		t.Fatalf("got: %+v", got)
	}
}

func TestCollector_ScrapeTraceSamplesDedupes(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string][]trace.JourneySample{
			"samples": {
				{RequestID: "x", Op: "/r", DurationMs: 1},
				{RequestID: "y", Op: "/r", DurationMs: 2},
			},
		})
	}))
	defer srv.Close()

	c := &MetricsCollector{scrapeClient: srv.Client()}
	// Override URL by using httptest host - scrape uses fixed cluster DNS, so test merge path directly.
	c.mergeTraceSamples("ns", []trace.JourneySample{{RequestID: "x", DurationMs: 1}})
	c.mergeTraceSamples("ns", []trace.JourneySample{{RequestID: "x", DurationMs: 5}})
	got := c.TraceSamplesFor("ns")
	if len(got) != 1 || got[0].DurationMs != 5 {
		t.Fatalf("got: %+v", got)
	}
}

func TestSessionSummary_IncludesTraceSamples(t *testing.T) {
	t.Parallel()
	c := &MetricsCollector{}
	c.mergeTraceSamples("ns", []trace.JourneySample{{RequestID: "a", Op: "/r"}})
	summary := c.SessionSummary("ns", nil, nil)
	raw, ok := summary["trace_samples"]
	if !ok {
		t.Fatal("missing trace_samples")
	}
	b, _ := json.Marshal(raw)
	var rows []trace.JourneySample
	if err := json.Unmarshal(b, &rows); err != nil || len(rows) != 1 {
		t.Fatalf("rows: %v err=%v", rows, err)
	}
}

func TestMaybeScrapeTraceSamples_EveryThirdCollect(t *testing.T) {
	t.Parallel()
	c := &MetricsCollector{scrapeClient: &http.Client{Timeout: 1}}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		c.maybeScrapeTraceSamples(ctx, "ns-tick")
	}
	if c.traceCollectTick["ns-tick"] != 3 {
		t.Fatalf("ticks: %d", c.traceCollectTick["ns-tick"])
	}
}
