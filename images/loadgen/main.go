// Command loadgen runs an HTTP load generator (Vegeta) for a single deployment namespace.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
)

const listenAddr = ":9100"

type server struct {
	mu sync.Mutex

	running  bool
	cancel   context.CancelFunc
	startedAt *time.Time

	statsMu sync.RWMutex
	stats   loadgen.LoadStats
	recent  loadgen.LatencyRing
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	srv := &server{
		stats: loadgen.LoadStats{
			StatusCodes:  map[string]int{},
			Distribution: map[string]int{},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /start", srv.handleStart)
	mux.HandleFunc("DELETE /stop", srv.handleStop)
	mux.HandleFunc("GET /metrics", srv.handleMetrics)

	httpServer := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("loadgen listening", "addr", listenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("listen failed", "error", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body loadgen.PodStartBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if body.RPS <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "rps must be positive"})
		return
	}
	if body.Actor == nil && len(body.Targets) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "targets or actor config required"})
		return
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "load already running"})
		return
	}

	if s.cancel != nil {
		s.cancel()
	}

	attackCtx, cancel := context.WithCancel(context.Background())
	s.running = true
	s.cancel = cancel
	now := time.Now()
	s.startedAt = &now

	s.statsMu.Lock()
	s.stats = loadgen.LoadStats{
		Active:       true,
		CurrentRPS:   float64(body.RPS),
		TargetRPS:    float64(body.RPS),
		ScheduleID:   body.ScheduleID,
		StatusCodes:  map[string]int{},
		Distribution: copyIntMap(body.Distribution),
		StartedAt:    &now,
	}
	s.recent = loadgen.LatencyRing{}
	s.statsMu.Unlock()
	s.mu.Unlock()

	go s.runAttack(attackCtx, body)

	writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
}

func (s *server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.running && s.cancel != nil {
		s.cancel()
	}
	s.running = false
	s.cancel = nil
	s.mu.Unlock()

	s.statsMu.Lock()
	s.stats.Active = false
	s.statsMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
}

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.statsMu.RLock()
	out := s.stats
	s.statsMu.RUnlock()
	writeJSON(w, http.StatusOK, out)
}

func (s *server) runAttack(ctx context.Context, body loadgen.PodStartBody) {
	defer func() {
		s.mu.Lock()
		s.running = false
		s.cancel = nil
		s.mu.Unlock()

		s.statsMu.Lock()
		s.stats.Active = false
		s.statsMu.Unlock()
	}()

	profile := loadgen.LoadProfile{
		RPS:             body.RPS,
		DurationSeconds: body.DurationSeconds,
		ScheduleID:      body.ScheduleID,
		Phases:          body.Phases,
		Distribution:    body.Distribution,
	}

	loadgen.RunAttackLoop(loadgen.AttackLoopOpts{
		Ctx:           ctx,
		Profile:       profile,
		BuildTargeter: loadgen.PodTargeterBuilder(body),
		Stats:         &s.stats,
		StatsMu:       &s.statsMu,
		Recent:        &s.recent,
		AttackName:    "cloud9-loadgen-pod",
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func copyIntMap(m map[string]int) map[string]int {
	if m == nil {
		return map[string]int{}
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
