// Command loadgen runs an HTTP load generator (Vegeta) for a single deployment namespace.
package main

import (
	"context"
	"encoding/json"
	"fmt"
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

	running   bool
	cancel    context.CancelFunc
	startedAt *time.Time
	control   *loadgen.AttackControl

	allowedDistKeys map[string]struct{}

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
	mux.HandleFunc("PATCH /rate", srv.handleRate)
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
	control := loadgen.NewAttackControl(body.RPS)
	control.SetBaseDistribution(body.Distribution)
	s.control = control
	s.allowedDistKeys = distKeySet(body.Distribution)
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

	go s.runAttack(attackCtx, body, control)

	writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
}

func (s *server) handleRate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RPS          *int           `json:"rps"`
		Distribution map[string]int `json:"distribution"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if body.RPS == nil && len(body.Distribution) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "rps and/or distribution required"})
		return
	}
	if body.RPS != nil {
		if err := loadgen.ValidateLiveRPS(*body.RPS); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}
	if len(body.Distribution) > 0 {
		if err := validatePodDistribution(s.allowedDistKeys, body.Distribution); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}

	s.mu.Lock()
	if !s.running || s.control == nil {
		s.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "load not running"})
		return
	}
	if body.RPS != nil {
		s.control.SetBaseRPS(*body.RPS)
	}
	if len(body.Distribution) > 0 {
		s.control.SetBaseDistribution(body.Distribution)
	}
	s.mu.Unlock()

	resp := map[string]any{"status": "updated"}
	if body.RPS != nil {
		resp["rps"] = *body.RPS
	}
	if len(body.Distribution) > 0 {
		resp["distribution"] = body.Distribution
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.running && s.cancel != nil {
		s.cancel()
	}
	s.running = false
	s.cancel = nil
	s.control = nil
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

func (s *server) runAttack(ctx context.Context, body loadgen.PodStartBody, control *loadgen.AttackControl) {
	defer func() {
		s.mu.Lock()
		s.running = false
		s.cancel = nil
		s.control = nil
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
		Control:       control,
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

func distKeySet(m map[string]int) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}

func validatePodDistribution(allowed map[string]struct{}, dist map[string]int) error {
	if len(allowed) == 0 {
		return fmt.Errorf("loadgen: no distribution keys configured")
	}
	for key := range allowed {
		if _, ok := dist[key]; !ok {
			return fmt.Errorf("loadgen: distribution missing endpoint %q", key)
		}
	}
	sum := 0
	for key, weight := range dist {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("loadgen: unknown distribution key %q", key)
		}
		if weight < 0 {
			return fmt.Errorf("loadgen: distribution weight for %q must be >= 0", key)
		}
		sum += weight
	}
	if sum <= 0 {
		return fmt.Errorf("loadgen: distribution weights must sum to greater than 0")
	}
	return nil
}
