package main

import (
	"net/http"
	"testing"
	"time"
)

func TestAPIHTTPClientBudget(t *testing.T) {
	c := newAPIHTTPClient()
	if c.Timeout != apiHTTPTimeout {
		t.Fatalf("timeout = %s, want %s", c.Timeout, apiHTTPTimeout)
	}
	if apiHTTPTimeout < 3*time.Minute {
		t.Fatalf("apiHTTPTimeout %s is below the cleanup budget that failed at 60s", apiHTTPTimeout)
	}
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type %T", c.Transport)
	}
	if tr.MaxIdleConnsPerHost < 32 {
		t.Fatalf("MaxIdleConnsPerHost = %d, want a burst-sized pool", tr.MaxIdleConnsPerHost)
	}
}
