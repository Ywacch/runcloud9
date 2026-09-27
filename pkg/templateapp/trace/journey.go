package trace

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// JourneySampleCap is how many sampled journeys each app pod retains in memory.
const JourneySampleCap = 16

// JourneySpan is one child hop in a sampled request (cache, db, or mq).
type JourneySpan struct {
	Component  string `json:"component"`
	Op         string `json:"op"`
	DurationMs int64  `json:"durationMs"`
}

// JourneySample is one end-to-end sampled HTTP request retained for /trace/samples.
type JourneySample struct {
	RequestID  string        `json:"requestId"`
	Op         string        `json:"op"`
	DurationMs int64         `json:"durationMs"`
	Spans      []JourneySpan `json:"spans"`
}

type journeyRecorder struct {
	requestID string
	op        string
	spans     []JourneySpan
}

type journeyKey struct{}

// Store retains a bounded ring of sampled journeys for scrape endpoints.
type Store struct {
	mu      sync.Mutex
	samples []JourneySample
}

// NewStore creates an empty journey sample store.
func NewStore() *Store {
	return &Store{}
}

var defaultStore = NewStore()

// DefaultStore returns the process-wide journey sample store used by template apps.
func DefaultStore() *Store {
	return defaultStore
}

// Record prepends sample and drops the oldest when over JourneySampleCap.
func (s *Store) Record(sample JourneySample) {
	if s == nil || sample.RequestID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = append([]JourneySample{sample}, s.samples...)
	if len(s.samples) > JourneySampleCap {
		s.samples = s.samples[:JourneySampleCap]
	}
}

// Samples returns a copy of retained journeys (newest first).
func (s *Store) Samples() []JourneySample {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]JourneySample, len(s.samples))
	copy(out, s.samples)
	return out
}

// BindJourney attaches an in-flight recorder when the request is trace-sampled.
func BindJourney(r *http.Request, op string) *http.Request {
	if r == nil || !Sampled(r.Context()) {
		return r
	}
	rid := RequestID(r.Context())
	if rid == "" {
		return r
	}
	rec := &journeyRecorder{requestID: rid, op: op}
	return r.WithContext(context.WithValue(r.Context(), journeyKey{}, rec))
}

// RecordSpan appends a child span for the active sampled journey in ctx.
func RecordSpan(ctx context.Context, component, op string, duration time.Duration) {
	rec, _ := ctx.Value(journeyKey{}).(*journeyRecorder)
	if rec == nil || component == "" || op == "" {
		return
	}
	ms := duration.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	rec.spans = append(rec.spans, JourneySpan{
		Component:  component,
		Op:         op,
		DurationMs: ms,
	})
}

// FinishJourney commits the sampled journey to DefaultStore.
func FinishJourney(ctx context.Context, total time.Duration) {
	rec, _ := ctx.Value(journeyKey{}).(*journeyRecorder)
	if rec == nil {
		return
	}
	ms := total.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	spans := append([]JourneySpan(nil), rec.spans...)
	defaultStore.Record(JourneySample{
		RequestID:  rec.requestID,
		Op:         rec.op,
		DurationMs: ms,
		Spans:      spans,
	})
}

// SamplesHandler serves GET JSON {"samples":[...]} from store.
func SamplesHandler(store *Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if store == nil {
			_ = json.NewEncoder(w).Encode(map[string][]JourneySample{"samples": {}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string][]JourneySample{"samples": store.Samples()})
	})
}
