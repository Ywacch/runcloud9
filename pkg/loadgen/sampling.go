package loadgen

import (
	"hash/fnv"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const (
	traceSampleHeader      = "X-Trace-Sample"
	defaultTraceSampleRate = 0.02
)

// TraceSampleHeader is the HTTP header marking a request for per-hop trace logging.
const TraceSampleHeader = traceSampleHeader

// TraceSampleValue is the header value indicating the request was sampled.
const TraceSampleValue = "1"

// traceUnit maps requestID to a stable uniform value in [0, 1).
func traceUnit(requestID string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(requestID))
	return float64(h.Sum64()%1_000_000) / 1_000_000
}

// ShouldSampleTrace decides head-based stratified sampling by comparing traceUnit(requestID)
// against a per-endpoint threshold. rates[endpointKey] overrides defaultRate so low-volume
// endpoints can be bumped independently.
func ShouldSampleTrace(requestID, endpointKey string, rates map[string]float64, defaultRate float64) bool {
	rate := defaultRate
	if rates != nil {
		if r, ok := rates[endpointKey]; ok {
			rate = r
		}
	}
	return sampleAtRate(requestID, rate)
}

func sampleAtRate(requestID string, rate float64) bool {
	switch {
	case rate <= 0:
		return false
	case rate >= 1:
		return true
	default:
		return traceUnit(requestID) < rate
	}
}

// TraceSampleRate reads TRACE_SAMPLE_RATE from the environment (default 0.02).
func TraceSampleRate() float64 {
	v := strings.TrimSpace(os.Getenv("TRACE_SAMPLE_RATE"))
	if v == "" {
		return defaultTraceSampleRate
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		return defaultTraceSampleRate
	}
	if f > 1 {
		return 1
	}
	return f
}

// ApplyTraceSampleHeaders sets X-Request-ID and X-Trace-Sample when sampled.
func ApplyTraceSampleHeaders(h http.Header, requestID, endpointKey string, rates map[string]float64, defaultRate float64) {
	h.Set("X-Request-ID", requestID)
	if ShouldSampleTrace(requestID, endpointKey, rates, defaultRate) {
		h.Set(traceSampleHeader, TraceSampleValue)
	}
}
