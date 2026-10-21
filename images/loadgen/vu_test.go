package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/loadgen/vu"
)

func TestHandleStart_withVU(t *testing.T) {
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user-pool":
			_ = json.NewEncoder(w).Encode(map[string][]int{"userIds": {1, 2}})
		case "/post":
			_ = json.NewEncoder(w).Encode(map[string]int{"post_id": 1})
		default:
			http.NotFound(w, r)
		}
	}))
	defer app.Close()

	s := &server{
		stats: loadgen.LoadStats{StatusCodes: map[string]int{}, Distribution: map[string]int{}},
	}
	body := loadgen.PodStartBody{
		RPS: 10,
		Actor: &loadgen.PodActorConfig{
			BaseURL: app.URL,
			Endpoints: []loadgen.EndpointSpec{
				{Method: "GET", PathTemplate: "/feed/{userId}", Weight: 100},
			},
			Distribution: map[string]int{"GET /feed/{userId}": 100},
		},
		VU: &loadgen.PodVUConfig{
			BaseURL: app.URL,
			UserIDs: []string{"1"},
			Actions: map[string]vu.Action{
				"create_root":  {Method: "POST", Path: "/post", Body: "{}", IDField: "post_id"},
				"create_reply": {Method: "POST", Path: "/post", Body: "{}", IDField: "post_id"},
			},
			Config: vu.Config{Behaviors: []vu.BehaviorConfig{
				{Name: "conversation_starter", Count: 1},
			}},
		},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/start", jsonBody(body))
	s.handleStart(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("start: %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	s.handleStop(rec, httptest.NewRequest(http.MethodDelete, "/stop", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("stop: %d", rec.Code)
	}
}

func TestHandleStart_badVU(t *testing.T) {
	s := &server{
		stats: loadgen.LoadStats{StatusCodes: map[string]int{}, Distribution: map[string]int{}},
	}
	body := loadgen.PodStartBody{
		RPS: 10,
		Actor: &loadgen.PodActorConfig{
			BaseURL: "http://example.com",
			Endpoints: []loadgen.EndpointSpec{
				{Method: "GET", PathTemplate: "/x", Weight: 100},
			},
			Distribution: map[string]int{"GET /x": 100},
		},
		VU: &loadgen.PodVUConfig{
			Config: vu.Config{Behaviors: []vu.BehaviorConfig{
				{Name: "conversation_starter", Count: 1},
			}},
		},
	}
	rec := httptest.NewRecorder()
	s.handleStart(rec, httptest.NewRequest(http.MethodPost, "/start", jsonBody(body)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
	if s.running {
		t.Fatal("server should not be running")
	}
}

func jsonBody(v any) *strings.Reader {
	b, _ := json.Marshal(v)
	return strings.NewReader(string(b))
}
