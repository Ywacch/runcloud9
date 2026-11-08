package main

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	if deleteRequestTimeout != 30*time.Second {
		t.Fatalf("delete timeout = %s, want 30s", deleteRequestTimeout)
	}
}

func TestDeleteDeployment_ParentDeadlineEndsTheCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()

	client := newAPIClient(srv.URL, "token")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := client.DeleteDeployment(ctx, "ns-1")
	if err == nil {
		t.Fatal("expected delete to stop when the parent deadline ends")
	}
	if time.Since(started) > time.Second {
		t.Fatalf("delete waited %s", time.Since(started))
	}
}
