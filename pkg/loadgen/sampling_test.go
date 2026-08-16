package loadgen

import (
	"net/http"
	"testing"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

func TestTraceUnit_deterministic(t *testing.T) {
	t.Parallel()
	const id = "abc123deadbeef"
	u1 := traceUnit(id)
	u2 := traceUnit(id)
	if u1 != u2 {
		t.Fatalf("traceUnit not deterministic: %v vs %v", u1, u2)
	}
	if u1 < 0 || u1 >= 1 {
		t.Fatalf("traceUnit out of range: %v", u1)
	}
}

func TestShouldSampleTrace_stratifiedRates(t *testing.T) {
	t.Parallel()
	rates := map[string]float64{
		"GET /health": 1.0,
		"GET /r/{code}": 0,
	}
	if !ShouldSampleTrace("any-id", "GET /health", rates, 0.02) {
		t.Fatal("expected GET /health always sampled at rate 1.0")
	}
	if ShouldSampleTrace("any-id", "GET /r/{code}", rates, 0.02) {
		t.Fatal("expected GET /r/{code} never sampled at rate 0")
	}
}

func TestShouldSampleTrace_respectsDefaultRate(t *testing.T) {
	t.Parallel()
	const id = "stable-sample-id"
	if !ShouldSampleTrace(id, "GET /feed/{userId}", nil, 1.0) {
		t.Fatal("expected sample at rate 1.0")
	}
	if ShouldSampleTrace(id, "GET /feed/{userId}", nil, 0) {
		t.Fatal("expected no sample at rate 0")
	}
}

func TestApplyTraceSampleHeaders_setsSampleWhenRateOne(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	ApplyTraceSampleHeaders(h, "rid-1", "GET /health", nil, 1.0)
	if got := h.Get("X-Request-ID"); got != "rid-1" {
		t.Fatalf("X-Request-ID: got %q", got)
	}
	if got := h.Get(TraceSampleHeader); got != TraceSampleValue {
		t.Fatalf("%s: got %q want %q", TraceSampleHeader, got, TraceSampleValue)
	}
}

func TestApplyTraceSampleHeaders_omitsSampleWhenRateZero(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	ApplyTraceSampleHeaders(h, "rid-2", "GET /health", nil, 0)
	if got := h.Get(TraceSampleHeader); got != "" {
		t.Fatalf("unexpected %s: %q", TraceSampleHeader, got)
	}
}

func TestBuildTargeter_setsTraceSampleAtFullRate(t *testing.T) {
	// Cannot use t.Parallel with t.Setenv.
	t.Setenv("TRACE_SAMPLE_RATE", "1")
	specs := []EndpointSpec{
		{Method: "GET", PathTemplate: "/health", Weight: 1},
	}
	targeter := buildTargeter("http://example.test", specs, map[string][]string{})
	var tgt vegeta.Target
	if err := targeter(&tgt); err != nil {
		t.Fatalf("targeter: %v", err)
	}
	if tgt.Header.Get(TraceSampleHeader) != TraceSampleValue {
		t.Fatalf("expected %s=%q at rate 1.0", TraceSampleHeader, TraceSampleValue)
	}
}
