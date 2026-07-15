package main

import (
	"net/http"
	"testing"
	"time"
)

func TestNewProxyClient_TunedTransport(t *testing.T) {
	t.Parallel()
	c := NewProxyClient("cache", "8081")
	if c.baseURL != "http://cache:8081" {
		t.Fatalf("baseURL: got %q", c.baseURL)
	}
	if c.httpClient.Timeout != 10*time.Second {
		t.Fatalf("Timeout: got %v", c.httpClient.Timeout)
	}
	tr, ok := c.httpClient.Transport.(*http.Transport)
	if !ok || tr == nil {
		t.Fatal("expected *http.Transport")
	}
	if tr.MaxIdleConnsPerHost < 64 {
		t.Fatalf("MaxIdleConnsPerHost: got %d want >= 64", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxIdleConns < 128 {
		t.Fatalf("MaxIdleConns: got %d want >= 128", tr.MaxIdleConns)
	}
}
