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
	vegeta "github.com/tsenart/vegeta/v12/lib"
)

const listenAddr = ":9100"

type startRequest struct {
	RPS             int            `json:"rps"`
	DurationSeconds int64          `json:"durationSeconds"`
	Targets         []targetInput  `json:"targets"`
	Distribution    map[string]int `json:"distribution,omitempty"`
}

type targetInput struct {
	URL    string `json:"url"`
	Method string `json:"method"`
	Body   string `json:"body"`
	Weight int    `json:"weight"`
	Key    string `json:"key,omitempty"`
}

type server struct {
	mu sync.Mutex

	running       bool
	cancel        context.CancelFunc
	configuredRPS int
	distribution  map[string]int
	startedAt     *time.Time

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

	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.RPS <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "rps must be positive"})
		return
	}
	if len(req.Targets) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "targets required"})
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
	s.configuredRPS = req.RPS
	s.distribution = copyIntMap(req.Distribution)
	now := time.Now()
	s.startedAt = &now

	s.statsMu.Lock()
	s.stats = loadgen.LoadStats{
		Active:       true,
		CurrentRPS:   float64(req.RPS),
		TargetRPS:    float64(req.RPS),
		StatusCodes:  map[string]int{},
		Distribution: copyIntMap(req.Distribution),
		StartedAt:    &now,
	}
	s.recent = loadgen.LatencyRing{}
	s.statsMu.Unlock()
	s.mu.Unlock()

	go s.runAttack(attackCtx, req)

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

func (s *server) runAttack(ctx context.Context, req startRequest) {
	defer func() {
		s.mu.Lock()
		s.running = false
		s.cancel = nil
		s.mu.Unlock()

		s.statsMu.Lock()
		s.stats.Active = false
		s.statsMu.Unlock()
	}()

	targeter := loadgen.BuildTargeterFromResolved(podTargetsToResolved(req.Targets))
	attacker := loadgen.NewVegetaAttacker(req.RPS)

	rate := vegeta.Rate{Freq: req.RPS, Per: time.Second}
	var attackDur time.Duration
	if req.DurationSeconds > 0 {
		attackDur = time.Duration(req.DurationSeconds) * time.Second
	}

	results := attacker.Attack(targeter, rate, attackDur, "cloud9-loadgen-pod")
	tick1s := time.NewTicker(time.Second)
	tick5s := time.NewTicker(loadgen.IntervalWindow)
	defer tick1s.Stop()
	defer tick5s.Stop()

	var cumulative vegeta.Metrics
	var window vegeta.Metrics
	var vmMu sync.Mutex

	for {
		select {
		case res, ok := <-results:
			if !ok {
				vmMu.Lock()
				s.statsMu.Lock()
				window = loadgen.CloseInterval(&s.stats, &window, time.Now())
				cumulative.Close()
				s.stats.RecentLatenciesMs = s.recent.Snapshot()
				loadgen.ApplyMetricsUpdate(&s.stats, &cumulative, req.RPS, s.startedAt)
				if len(s.distribution) > 0 {
					s.stats.Distribution = copyIntMap(s.distribution)
				}
				s.statsMu.Unlock()
				vmMu.Unlock()
				return
			}
			vmMu.Lock()
			cumulative.Add(res)
			window.Add(res)
			s.recent.Push(res.Latency.Seconds() * 1000)
			vmMu.Unlock()

		case <-tick1s.C:
			vmMu.Lock()
			s.statsMu.Lock()
			cumulative.Close()
			s.stats.RecentLatenciesMs = s.recent.Snapshot()
			loadgen.ApplyMetricsUpdate(&s.stats, &cumulative, req.RPS, s.startedAt)
			if len(s.distribution) > 0 {
				s.stats.Distribution = copyIntMap(s.distribution)
			}
			s.statsMu.Unlock()
			vmMu.Unlock()

		case now := <-tick5s.C:
			vmMu.Lock()
			s.statsMu.Lock()
			window = loadgen.CloseInterval(&s.stats, &window, now)
			s.statsMu.Unlock()
			vmMu.Unlock()

		case <-ctx.Done():
			attacker.Stop()
			return
		}
	}
}

func podTargetsToResolved(targets []targetInput) []loadgen.ResolvedTarget {
	out := make([]loadgen.ResolvedTarget, 0, len(targets))
	for _, t := range targets {
		out = append(out, loadgen.ResolvedTarget{
			URL:    t.URL,
			Method: t.Method,
			Body:   t.Body,
			Weight: t.Weight,
			Key:    t.Key,
		})
	}
	return out
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
