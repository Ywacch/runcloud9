package loadgen

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPodLoadGenerator_StartStopGetStats(t *testing.T) {
	var running bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/code-pool":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string][]string{"codes": {"abc123"}})
		case r.Method == http.MethodPost && r.URL.Path == "/start":
			if running {
				w.WriteHeader(http.StatusConflict)
				return
			}
			running = true
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "started"})
		case r.Method == http.MethodDelete && r.URL.Path == "/stop":
			running = false
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPatch && r.URL.Path == "/rate":
			if !running {
				w.WriteHeader(http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
		case r.Method == http.MethodGet && r.URL.Path == "/metrics":
			_ = json.NewEncoder(w).Encode(LoadStats{
				Active:      running,
				CurrentRPS:  50,
				SuccessRate: 100,
				StatusCodes: map[string]int{"200": 10},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	podEndpointOverride = func(string) string { return srv.URL }
	t.Cleanup(func() { podEndpointOverride = nil })

	p := &PodLoadGenerator{httpClient: srv.Client()}
	profile := NormalizeProfile(LoadProfile{
		Template: "url-shortener",
		RPS:      50,
		BaseURL:  srv.URL,
	})

	if err := p.Start(context.Background(), "ns-test", profile); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Update(context.Background(), "ns-test", LiveUpdate{RPS: intPtr(80)}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	stats := p.GetStats("ns-test")
	if !stats.Active {
		t.Fatalf("GetStats active: got %+v", stats)
	}
	if err := p.Stop(context.Background(), "ns-test"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func intPtr(n int) *int { return &n }
