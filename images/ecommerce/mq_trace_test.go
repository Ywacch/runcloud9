package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAmqpTraceHeaders_sampled(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/checkout", nil)
	req.Header.Set("X-Request-ID", "rid-1")
	req.Header.Set("X-Trace-Sample", "1")
	req = withRequestContext(req)

	h := amqpTraceHeaders(req.Context())
	if h["x-request-id"] != "rid-1" {
		t.Fatalf("x-request-id: %+v", h)
	}
	if h["x-trace-sample"] != "1" {
		t.Fatalf("x-trace-sample: %+v", h)
	}
}

func TestAmqpTraceHeaders_unsampled(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/checkout", nil)
	req.Header.Set("X-Request-ID", "rid-2")
	req = withRequestContext(req)

	h := amqpTraceHeaders(req.Context())
	if h["x-request-id"] != "rid-2" {
		t.Fatalf("x-request-id: %+v", h)
	}
	if _, ok := h["x-trace-sample"]; ok {
		t.Fatalf("did not expect sample header: %+v", h)
	}
}

func TestAmqpTraceHeaders_empty(t *testing.T) {
	h := amqpTraceHeaders(context.Background())
	if h != nil {
		t.Fatalf("want nil headers, got %+v", h)
	}
}
