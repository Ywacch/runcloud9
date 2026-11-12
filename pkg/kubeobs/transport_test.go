package kubeobs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method   string
		path     string
		wantVerb string
		wantRes  string
	}{
		{http.MethodGet, `/api/v1/namespaces/cloud9-x/pods`, "list", "pods"},
		{http.MethodGet, `/api/v1/namespaces/cloud9-x/pods/app-server-abc`, "get", "pods"},
		{http.MethodPost, `/api/v1/namespaces`, "create", "namespaces"},
		{http.MethodDelete, `/api/v1/namespaces/cloud9-x`, "delete", "namespaces"},
		{http.MethodPost, `/apis/apps/v1/namespaces/cloud9-x/deployments`, "create", "deployments"},
		{http.MethodGet, `/apis/apps/v1/namespaces/cloud9-x/statefulsets`, "list", "statefulsets"},
		{http.MethodGet, `/apis/autoscaling/v2/namespaces/cloud9-x/horizontalpodautoscalers`, "list", "horizontalpodautoscalers"},
		{http.MethodGet, `/api/v1/namespaces/cloud9-x/configmaps/lb`, "get", "configmaps"},
		{http.MethodPatch, `/api/v1/namespaces/cloud9-x/configmaps/lb`, "patch", "configmaps"},
		{http.MethodGet, `/api/v1/namespaces/cloud9-x/endpoints/app-server`, "get", "endpoints"},
		{http.MethodGet, `/apis/metrics.k8s.io/v1beta1/pods`, "list", "pods"},
		{http.MethodGet, `/apis/metrics.k8s.io/v1beta1/namespaces/cloud9-x/pods`, "list", "pods"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			verb, resource := Classify(tc.method, tc.path)
			if verb != tc.wantVerb || resource != tc.wantRes {
				t.Fatalf("Classify(%q, %q) = (%q, %q), want (%q, %q)", tc.method, tc.path, verb, resource, tc.wantVerb, tc.wantRes)
			}
		})
	}
}

type recordingObserver struct {
	requests  []observedRequest
	throttles int
}

type observedRequest struct {
	verb, resource, caller string
	seconds                float64
}

func (o *recordingObserver) ObserveRequest(verb, resource, caller string, seconds float64) {
	o.requests = append(o.requests, observedRequest{verb, resource, caller, seconds})
}

func (o *recordingObserver) ObserveThrottle(waitSeconds float64) {
	if waitSeconds > 0 {
		o.throttles++
	}
}

func (o *recordingObserver) ObserveThrottleWait(verb, resource, caller string, seconds float64) {
	o.requests = append(o.requests, observedRequest{verb, resource, caller, seconds})
}

func TestWrap_RecordsCaller(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	obs := &recordingObserver{}
	rt := Wrap(http.DefaultTransport, obs)

	req, err := http.NewRequestWithContext(WithCaller(context.Background(), "metrics"), http.MethodGet, srv.URL+"/api/v1/namespaces/x/pods", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if len(obs.requests) != 1 {
		t.Fatalf("requests: want 1, got %d", len(obs.requests))
	}
	if obs.requests[0].caller != "metrics" {
		t.Fatalf("caller: want metrics, got %q", obs.requests[0].caller)
	}
	if obs.requests[0].verb != "list" || obs.requests[0].resource != "pods" {
		t.Fatalf("labels: got %q %q", obs.requests[0].verb, obs.requests[0].resource)
	}

	req2, err := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/namespaces/x/pods", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := rt.RoundTrip(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if len(obs.requests) != 2 {
		t.Fatalf("requests: want 2, got %d", len(obs.requests))
	}
	if obs.requests[1].caller != "unknown" {
		t.Fatalf("caller: want unknown, got %q", obs.requests[1].caller)
	}
}

type slowWaitLimiter struct {
	wait time.Duration
}

func (s *slowWaitLimiter) TryAccept() bool { return false }

func (s *slowWaitLimiter) Accept() {}

func (s *slowWaitLimiter) Stop() {}

func (s *slowWaitLimiter) QPS() float32 { return 1 }

func (s *slowWaitLimiter) Wait(ctx context.Context) error {
	select {
	case <-time.After(s.wait):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestCountingLimiter_CountsASlowWait(t *testing.T) {
	t.Parallel()

	obs := &recordingObserver{}
	lim := CountingLimiter(&slowWaitLimiter{wait: 20 * time.Millisecond}, obs)
	if err := lim.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if obs.throttles != 1 {
		t.Fatalf("throttles: want 1, got %d", obs.throttles)
	}

	fast := &recordingObserver{}
	fastLim := CountingLimiter(&slowWaitLimiter{wait: 0}, fast)
	if err := fastLim.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fast.throttles != 0 {
		t.Fatalf("throttles: want 0, got %d", fast.throttles)
	}
}

func TestWithCaller(t *testing.T) {
	t.Parallel()
	ctx := WithCaller(context.Background(), "deployer")
	if got := CallerFrom(ctx); got != "deployer" {
		t.Fatalf("CallerFrom: want deployer, got %q", got)
	}
	if got := CallerFrom(context.Background()); got != "" {
		t.Fatalf("empty ctx caller: want empty, got %q", got)
	}
}
