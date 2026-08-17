package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestShouldTrace_sampleHeaderWithoutVerbose(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	req := httptest.NewRequest(http.MethodGet, "/get/abc", nil)
	req.Header.Set("X-Request-ID", "trace-proxy-1")
	req.Header.Set("X-Trace-Sample", "1")
	req = withRequestContext(req)
	if !shouldTrace(req.Context()) {
		t.Fatal("expected shouldTrace true when X-Trace-Sample is set")
	}
	if requestIDFrom(req.Context()) != "trace-proxy-1" {
		t.Fatalf("request id: %q", requestIDFrom(req.Context()))
	}
}

func TestShouldTrace_neitherVerboseNorSample(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	req := httptest.NewRequest(http.MethodGet, "/get/abc", nil)
	req.Header.Set("X-Request-ID", "trace-proxy-2")
	req = withRequestContext(req)
	if shouldTrace(req.Context()) {
		t.Fatal("expected shouldTrace false without sample or verbose")
	}
}

func TestShouldTrace_verboseWithoutSample(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "1")
	req := httptest.NewRequest(http.MethodGet, "/get/abc", nil)
	req.Header.Set("X-Request-ID", "trace-proxy-3")
	req = withRequestContext(req)
	if !shouldTrace(req.Context()) {
		t.Fatal("expected shouldTrace true when LOG_VERBOSE=1")
	}
}
