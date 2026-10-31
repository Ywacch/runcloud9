package trace

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStore_DropsOldestAfterCap(t *testing.T) {
	t.Parallel()
	s := NewStore()
	for i := 0; i < JourneySampleCap+1; i++ {
		s.Record(JourneySample{RequestID: fmt.Sprintf("req-%d", i), Op: "/r"})
	}
	got := s.Samples()
	if len(got) != JourneySampleCap {
		t.Fatalf("len: %d want %d", len(got), JourneySampleCap)
	}
	if got[0].RequestID != fmt.Sprintf("req-%d", JourneySampleCap) {
		t.Fatalf("newest first: %+v", got[0])
	}
	if got[len(got)-1].RequestID != "req-1" {
		t.Fatalf("oldest retained: %+v (req-0 should be dropped)", got[len(got)-1])
	}
}

func TestFinishJourney_RecordsSpans(t *testing.T) {
	prev := defaultStore
	t.Cleanup(func() { defaultStore = prev })
	s := NewStore()
	defaultStore = s

	req := httptest.NewRequest(http.MethodGet, "/r/x", nil)
	req.Header.Set("X-Request-ID", "rid-1")
	req.Header.Set("X-Trace-Sample", "1")
	req = ContextFromRequest(req)
	req = BindJourney(req, "/r")
	ctx := req.Context()
	RecordSpan(ctx, "cache", "cache_get", 2*time.Millisecond)
	FinishJourney(ctx, 5*time.Millisecond)

	samples := s.Samples()
	if len(samples) != 1 {
		t.Fatalf("samples: %+v", samples)
	}
	if samples[0].RequestID != "rid-1" || samples[0].Op != "/r" {
		t.Fatalf("sample: %+v", samples[0])
	}
	if len(samples[0].Spans) != 1 || samples[0].Spans[0].Component != "cache" {
		t.Fatalf("spans: %+v", samples[0].Spans)
	}
}

func TestSamplesHandler_JSON(t *testing.T) {
	t.Parallel()
	s := NewStore()
	s.Record(JourneySample{RequestID: "a", Op: "/post", DurationMs: 3})
	rec := httptest.NewRecorder()
	SamplesHandler(s).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/trace/samples", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"requestId":"a"`) {
		t.Fatalf("body: %s", rec.Body.String())
	}
}

func TestBindJourney_skipsWithoutSampleHeader(t *testing.T) {
	prev := defaultStore
	t.Cleanup(func() { defaultStore = prev })
	defaultStore = NewStore()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "rid")
	req = ContextFromRequest(req)
	req = BindJourney(req, "/")
	FinishJourney(req.Context(), time.Millisecond)
	if len(DefaultStore().Samples()) != 0 {
		t.Fatal("expected no samples without trace sample header")
	}
}
