// Duplicate of pkg/loadgen/sampling.go — collapse into pkg/loadgen via replace in PR3.
package main

import (
	"hash/fnv"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const (
	traceSampleHeader      = "X-Trace-Sample"
	traceSampleValue       = "1"
	defaultTraceSampleRate = 0.02
)

func traceUnit(requestID string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(requestID))
	return float64(h.Sum64()%1_000_000) / 1_000_000
}

func shouldSampleTrace(requestID, endpointKey string, rates map[string]float64, defaultRate float64) bool {
	rate := defaultRate
	if rates != nil {
		if r, ok := rates[endpointKey]; ok {
			rate = r
		}
	}
	switch {
	case rate <= 0:
		return false
	case rate >= 1:
		return true
	default:
		return traceUnit(requestID) < rate
	}
}

func traceSampleRate() float64 {
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

func applyTraceSampleHeaders(h http.Header, requestID, endpointKey string, rates map[string]float64, defaultRate float64) {
	h.Set("X-Request-ID", requestID)
	if shouldSampleTrace(requestID, endpointKey, rates, defaultRate) {
		h.Set(traceSampleHeader, traceSampleValue)
	}
}
