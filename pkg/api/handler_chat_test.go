package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/inference"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
)

type stubInferenceService struct {
	chatResp *inference.ChatResponse
	chatErr  error
}

func (s *stubInferenceService) ReelCaptions(context.Context, inference.ReelCaptionsRequest) (*inference.ReelCaptionsResponse, error) {
	return nil, nil
}

func (s *stubInferenceService) Postmortem(context.Context, any) (*inference.PostmortemResponse, error) {
	return nil, nil
}

func (s *stubInferenceService) Chat(_ context.Context, _ inference.ChatRequest) (*inference.ChatResponse, error) {
	if s.chatErr != nil {
		return nil, s.chatErr
	}
	return s.chatResp, nil
}

func TestHandleChatStream_VerifiedReply(t *testing.T) {
	t.Parallel()

	ns := "ns-chat-live"
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				UserID:    "dev-user",
				Template:  "url-shortener",
				Status:    "running",
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
			},
		},
	}
	collector := metrics.NewMetricsCollector("", nil, nil, nil, nil)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	collector.AppendSessionSnapshots(ns, metrics.DeploymentMetrics{
		Timestamp: t0,
		Components: map[string]metrics.ComponentMetrics{
			"db": {Component: "db", CPUUsage: 0.42, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
		},
	})

	st.users = map[string]store.UserRecord{
		"dev-user": {IsInternal: true},
	}
	h := NewHandler(nil, st, nil, nil, collector, nil, nil, nil, nil)
	h.inferenceClient = &stubInferenceService{
		chatResp: &inference.ChatResponse{
			Status: "ok",
			Reply:  "Database CPU is elevated.",
			Claims: []inference.WireClaim{
				{
					Scope:      "component",
					Components: []string{"db"},
					Aspect:     "what",
					Text:       "Database CPU is elevated.",
					FactRefs:   []string{"fact:metric:db.cpuUsage.last"},
					Grounding:  "inferred",
				},
			},
			Usage: inference.TokenUsage{TotalTokens: 10},
		},
	}

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	server := httptest.NewServer(mux)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/deployments/" + ns + "/chat"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	var sessionFrame map[string]any
	if err := conn.ReadJSON(&sessionFrame); err != nil {
		t.Fatalf("session frame: %v", err)
	}
	if sessionFrame["type"] != "session" {
		t.Fatalf("session frame: %+v", sessionFrame)
	}

	_ = conn.WriteJSON(map[string]string{"type": "user_message", "text": "What is happening on the database?"})

	var sawDelta, sawEnd bool
	var endReply string
	for i := 0; i < 20; i++ {
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		switch frame["type"] {
		case "assistant_delta":
			sawDelta = true
		case "assistant_end":
			sawEnd = true
			endReply, _ = frame["reply"].(string)
		}
		if sawEnd {
			break
		}
	}
	if !sawDelta || !sawEnd {
		t.Fatalf("delta=%v end=%v", sawDelta, sawEnd)
	}
	if endReply == "" {
		t.Fatal("expected reply text")
	}
}

func TestHandleChatStream_RejectsTerminatedSession(t *testing.T) {
	t.Parallel()

	ns := "ns-chat-done"
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				UserID:    "dev-user",
				Status:    "terminated",
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, nil, nil, nil)
	h.inferenceClient = &stubInferenceService{}

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/deployments/"+ns+"/chat", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status: want 409, got %d", rec.Code)
	}
}

func TestHandleChatStream_RejectsNonInternalUser(t *testing.T) {
	t.Parallel()

	ns := "ns-chat-free"
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				UserID:    "dev-user",
				Status:    "running",
			},
		},
		users: map[string]store.UserRecord{
			"dev-user": {IsInternal: false},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, nil, nil, nil)
	h.inferenceClient = &stubInferenceService{}

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/deployments/"+ns+"/chat", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status: want 403, got %d body %s", rec.Code, rec.Body.String())
	}
}
