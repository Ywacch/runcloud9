package consistency

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
		want     bool
	}{
		{name: "cache-aside", strategy: "cache-aside", want: true},
		{name: "write-through", strategy: "write-through", want: true},
		{name: "write-behind", strategy: "write-behind", want: false},
		{name: "write-behind mixed case", strategy: "Write-Behind", want: false},
		{name: "empty", strategy: "", want: true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Enabled(tc.strategy); got != tc.want {
				t.Fatalf("Enabled(%q) = %v, want %v", tc.strategy, got, tc.want)
			}
		})
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

func TestVerifier_writeBehindSkipsTruthReader(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	v := New(Config{
		Template: "ecommerce",
		Source:   "cache",
		Strategy: "write-behind",
	}, func(context.Context, string) (string, error) {
		calls.Add(1)
		return "truth", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v.Start(ctx)

	v.Observe(ReadObservation{Key: "product:1", Served: "old", RequestID: "req-1"})
	time.Sleep(50 * time.Millisecond)

	if calls.Load() != 0 {
		t.Fatalf("truth reader calls = %d, want 0", calls.Load())
	}
	if len(v.Samples()) != 0 {
		t.Fatalf("samples = %+v, want empty", v.Samples())
	}
	v.Stop()
}

func TestVerifier_staleHit(t *testing.T) {
	t.Parallel()

	v := New(Config{
		Template:  "ecommerce",
		Source:    "cache",
		Strategy:  "cache-aside",
		QueueSize: 4,
	}, func(context.Context, string) (string, error) {
		return "truth", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v.Start(ctx)

	v.Observe(ReadObservation{
		Key: "product:42", Source: "cache", Served: "stale", RequestID: "req-stale",
	})

	waitFor(t, func() bool { return len(v.Samples()) == 1 })
	s := v.Samples()[0]
	if s.Key != "product:42" || s.Served != "stale" || s.Truth != "truth" || s.RequestID != "req-stale" {
		t.Fatalf("sample: %+v", s)
	}
	v.Stop()
}

func TestVerifier_freshHit(t *testing.T) {
	t.Parallel()

	var processed atomic.Int32
	v := New(Config{
		Template:  "ecommerce",
		Source:    "cache",
		Strategy:  "cache-aside",
		QueueSize: 4,
	}, func(context.Context, string) (string, error) {
		processed.Add(1)
		return "fresh", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v.Start(ctx)

	v.Observe(ReadObservation{
		Key: "product:7", Source: "cache", Served: "fresh", RequestID: "req-fresh",
	})

	waitFor(t, func() bool { return processed.Load() == 1 })
	if len(v.Samples()) != 0 {
		t.Fatalf("samples = %+v, want empty", v.Samples())
	}
	v.Stop()
}

func TestVerifier_dropOnFullNoDeadlock(t *testing.T) {
	t.Parallel()

	block := make(chan struct{})
	v := New(Config{
		Template:      "ecommerce",
		Source:        "cache",
		Strategy:      "cache-aside",
		QueueSize:     1,
		VerifyTimeout: 2 * time.Second,
	}, func(ctx context.Context, key string) (string, error) {
		if key == "block" {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-block:
				return "ok", nil
			}
		}
		return "ok", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v.Start(ctx)

	v.Observe(ReadObservation{Key: "block", Served: "x", RequestID: "r1"})
	for i := 0; i < 8; i++ {
		v.Observe(ReadObservation{Key: "product:1", Served: "x", RequestID: "r2"})
	}
	close(block)

	done := make(chan struct{})
	go func() {
		v.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked after drop-on-full flood")
	}
}

func TestVerifier_observeReturnsBeforeTruthReader(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	v := New(Config{
		Template:  "ecommerce",
		Source:    "cache",
		Strategy:  "cache-aside",
		QueueSize: 4,
	}, func(ctx context.Context, _ string) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "t", nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v.Start(ctx)

	finished := make(chan struct{})
	go func() {
		v.Observe(ReadObservation{Key: "k", Served: "s", RequestID: "r"})
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Observe blocked waiting for truth reader")
	}

	close(release)
	v.Stop()
}

func TestSamplesHandler_JSON(t *testing.T) {
	t.Parallel()

	v := New(Config{
		Template: "ecommerce",
		Source:   "cache",
		Strategy: "cache-aside",
	}, func(context.Context, string) (string, error) {
		return "db-val", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v.Start(ctx)

	v.Observe(ReadObservation{
		Key: "product:3", Source: "cache", Served: "cache-val", RequestID: "req-json",
	})
	waitFor(t, func() bool { return len(v.Samples()) == 1 })

	req := httptest.NewRequest(http.MethodGet, "/consistency/samples", nil)
	rec := httptest.NewRecorder()
	SamplesHandler(v, nil).ServeHTTP(rec, req)

	var body struct {
		Samples []Sample `json:"samples"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(body.Samples) != 1 {
		t.Fatalf("samples len = %d", len(body.Samples))
	}
	if body.Samples[0].Key != "product:3" || body.Samples[0].Served != "cache-val" ||
		body.Samples[0].Truth != "db-val" || body.Samples[0].RequestID != "req-json" {
		t.Fatalf("sample: %+v", body.Samples[0])
	}

	recNil := httptest.NewRecorder()
	SamplesHandler(nil, nil).ServeHTTP(recNil, httptest.NewRequest(http.MethodGet, "/", nil))
	var empty struct {
		Samples []Sample `json:"samples"`
	}
	if err := json.Unmarshal(recNil.Body.Bytes(), &empty); err != nil {
		t.Fatalf("nil handler unmarshal: %v", err)
	}
	if len(empty.Samples) != 0 {
		t.Fatalf("nil handler samples = %+v", empty.Samples)
	}

	v.Stop()
}

func TestVerifier_stopCancelsInFlightTruthReader(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	v := New(Config{
		Template:      "ecommerce",
		Source:        "cache",
		Strategy:      "cache-aside",
		QueueSize:     2,
		VerifyTimeout: 5 * time.Second,
	}, func(ctx context.Context, _ string) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	v.Start(ctx)

	v.Observe(ReadObservation{Key: "k", Served: "s", RequestID: "r"})
	<-started

	v.Stop()
	cancel()
}

func TestVerifier_skipsEmptyKey(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	v := New(Config{Template: "ecommerce", Source: "cache", Strategy: "cache-aside"}, func(context.Context, string) (string, error) {
		calls.Add(1)
		return "", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v.Start(ctx)

	v.Observe(ReadObservation{Key: "  ", Served: "x"})
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("calls = %d", calls.Load())
	}
	v.Stop()
}

func TestVerifier_truncatesLongEvidence(t *testing.T) {
	t.Parallel()

	long := stringsRepeat("a", 300)
	v := New(Config{
		Template: "ecommerce",
		Source:   "cache",
		Strategy: "cache-aside",
	}, func(context.Context, string) (string, error) {
		return "short", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v.Start(ctx)

	v.Observe(ReadObservation{Key: "k", Served: long, RequestID: "r"})
	waitFor(t, func() bool { return len(v.Samples()) == 1 })
	if len([]rune(v.Samples()[0].Served)) != maxEvidenceRunes {
		t.Fatalf("served runes = %d, want %d", len([]rune(v.Samples()[0].Served)), maxEvidenceRunes)
	}
	v.Stop()
}

func stringsRepeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}
