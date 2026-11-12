package kubeobs

import (
	"context"
	"net/url"
	"testing"
	"time"
)

func TestThrottleLatency_Observe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		verb       string
		path       string
		caller     string
		wait       time.Duration
		wantVerb   string
		wantRes    string
		wantCaller string
		wantN      int
	}{
		{
			name:       "namespace get without caller",
			verb:       "GET",
			path:       "/api/v1/namespaces/cloud9-url",
			wait:       3 * time.Millisecond,
			wantVerb:   "get",
			wantRes:    "namespaces",
			wantCaller: "unknown",
			wantN:      1,
		},
		{
			name:       "namespace delete from teardown",
			verb:       "DELETE",
			path:       "/api/v1/namespaces/cloud9-url",
			caller:     "teardown",
			wait:       60 * time.Millisecond,
			wantVerb:   "delete",
			wantRes:    "namespaces",
			wantCaller: "teardown",
			wantN:      1,
		},
		{
			name:  "negative wait is dropped",
			verb:  "GET",
			path:  "/api/v1/namespaces/cloud9-url",
			wait:  -time.Millisecond,
			wantN: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			obs := &recordingObserver{}
			ctx := context.Background()
			if tc.caller != "" {
				ctx = WithCaller(ctx, tc.caller)
			}
			u := url.URL{Path: tc.path}
			ThrottleLatency{Obs: obs}.Observe(ctx, tc.verb, u, tc.wait)
			if len(obs.requests) != tc.wantN {
				t.Fatalf("observations: got %d, want %d", len(obs.requests), tc.wantN)
			}
			if tc.wantN == 0 {
				return
			}
			got := obs.requests[0]
			if got.verb != tc.wantVerb || got.resource != tc.wantRes || got.caller != tc.wantCaller {
				t.Fatalf("labels: got %s %s %s, want %s %s %s", got.verb, got.resource, got.caller, tc.wantVerb, tc.wantRes, tc.wantCaller)
			}
			if got.seconds != tc.wait.Seconds() {
				t.Fatalf("seconds: got %v, want %v", got.seconds, tc.wait.Seconds())
			}
		})
	}
}

func TestThrottleLatency_NilObserver(t *testing.T) {
	t.Parallel()
	ThrottleLatency{}.Observe(context.Background(), "GET", url.URL{Path: "/api/v1/namespaces"}, time.Millisecond)
}
