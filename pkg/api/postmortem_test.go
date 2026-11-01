package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/inference"
	"github.com/ywacch/cloud9/pkg/store"
)

type postmortemStub struct {
	stubInferenceService
	resp *inference.PostmortemResponse
}

func (s *postmortemStub) Postmortem(context.Context, any) (*inference.PostmortemResponse, error) {
	s.postmortemCalls.Add(1)
	return s.resp, nil
}

func TestRunPostmortemAfterTeardown_StampsPendingThenResult(t *testing.T) {
	ns := "ns-pm-pending-ok"
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				UserID:    "dev-user",
				Template:  "url-shortener",
				Status:    "terminated",
				Design: map[string]any{
					"template": "url-shortener",
					"nodes": []any{
						map[string]any{
							"id":   "db1",
							"type": "db",
							"config": map[string]any{
								"readConsistency": "replica",
							},
						},
					},
				},
				MetricsSummary: map[string]any{
					"sentinel": "keep-me",
					"session_history": []any{
						map[string]any{
							"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
							"components": map[string]any{
								"db": map[string]any{
									"component": "db",
									"cpuUsage":  0.4,
									"cpuLimit":  1.0,
								},
							},
						},
					},
				},
			},
		},
		users: map[string]store.UserRecord{
			"dev-user": {IsInternal: true},
		},
	}
	stub := &postmortemStub{
		resp: &inference.PostmortemResponse{
			Status:  PostmortemStatusOK,
			Summary: "Steady cache pressure after warmup.",
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, nil, nil, nil)
	h.inferenceClient = stub

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.runPostmortemAfterTeardown(ns)
	}()
	wg.Wait()

	row, err := st.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if row.MetricsSummary["sentinel"] != "keep-me" {
		t.Fatalf("sentinel lost: %+v", row.MetricsSummary)
	}
	raw, ok := row.MetricsSummary["ai_postmortem"].(map[string]any)
	if !ok {
		t.Fatalf("ai_postmortem missing: %+v", row.MetricsSummary)
	}
	if raw["status"] != PostmortemStatusOK {
		t.Fatalf("status: got %v want %q", raw["status"], PostmortemStatusOK)
	}
	if stub.postmortemCalls.Load() != 1 {
		t.Fatalf("postmortem calls: got %d want 1", stub.postmortemCalls.Load())
	}
}

func TestRunPostmortemAfterTeardown_NonInternalNoStamp(t *testing.T) {
	ns := "ns-pm-no-stamp"
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace:      ns,
				UserID:         "free-user",
				Status:         "terminated",
				MetricsSummary: map[string]any{},
			},
		},
		users: map[string]store.UserRecord{
			"free-user": {},
		},
	}
	stub := &postmortemStub{
		resp: &inference.PostmortemResponse{Status: PostmortemStatusOK, Summary: "x"},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, nil, nil, nil)
	h.inferenceClient = stub
	h.runPostmortemAfterTeardown(ns)

	row, err := st.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := row.MetricsSummary["ai_postmortem"]; ok {
		t.Fatalf("unexpected ai_postmortem for non-internal: %+v", row.MetricsSummary)
	}
	if stub.postmortemCalls.Load() != 0 {
		t.Fatalf("postmortem calls: got %d want 0", stub.postmortemCalls.Load())
	}
}
