package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

func TestShouldTrace_sampleHeaderWithoutVerbose(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	req := httptest.NewRequest(http.MethodGet, "/feed/1", nil)
	req.Header.Set("X-Request-ID", "trace-test-1")
	req.Header.Set("X-Trace-Sample", "1")
	req = trace.ContextFromRequest(req)
	if !trace.ShouldTrace(req.Context()) {
		t.Fatal("expected shouldTrace true when X-Trace-Sample is set")
	}
}

func TestShouldTrace_verboseWithoutSample(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "1")
	req := httptest.NewRequest(http.MethodGet, "/feed/1", nil)
	req.Header.Set("X-Request-ID", "trace-test-2")
	req = trace.ContextFromRequest(req)
	if !trace.ShouldTrace(req.Context()) {
		t.Fatal("expected shouldTrace true when LOG_VERBOSE=1")
	}
}

func TestShouldTrace_neitherVerboseNorSample(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	req := httptest.NewRequest(http.MethodGet, "/feed/1", nil)
	req.Header.Set("X-Request-ID", "trace-test-3")
	req = trace.ContextFromRequest(req)
	if trace.ShouldTrace(req.Context()) {
		t.Fatal("expected shouldTrace false without sample or verbose")
	}
}

func TestSetRequestIDHeader_forwardsTraceSample(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/feed/1", nil)
	req.Header.Set("X-Request-ID", "downstream-rid")
	req.Header.Set("X-Trace-Sample", "1")
	req = trace.ContextFromRequest(req)

	out := httptest.NewRequest(http.MethodGet, "http://cache:6379/get", nil)
	trace.SetOutboundHeaders(out, req.Context())

	if got := out.Header.Get("X-Request-ID"); got != "downstream-rid" {
		t.Fatalf("X-Request-ID: got %q", got)
	}
	if got := out.Header.Get("X-Trace-Sample"); got != "1" {
		t.Fatalf("X-Trace-Sample: got %q", got)
	}
}
