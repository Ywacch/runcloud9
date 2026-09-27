package consistency

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const defaultQueueSize = 32
const defaultSampleCap = 8
const defaultVerifyTimeout = time.Second
const maxEvidenceRunes = 256

// Verifier compares sampled cache hits against TruthReader on a bounded background worker.
type Verifier struct {
	read          TruthReader
	equal         func(served, truth string) bool
	template      string
	source        string
	strategy      string
	queueSize     int
	sampleCap     int
	verifyTimeout time.Duration

	queue   chan ReadObservation
	mu      sync.RWMutex
	samples []Sample

	stopCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// New builds a verifier. Call Start before Observe and Stop on shutdown.
func New(cfg Config, read TruthReader) *Verifier {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.SampleCap <= 0 {
		cfg.SampleCap = defaultSampleCap
	}
	if cfg.VerifyTimeout <= 0 {
		cfg.VerifyTimeout = defaultVerifyTimeout
	}
	if cfg.Equal == nil {
		cfg.Equal = func(served, truth string) bool {
			return strings.TrimSpace(served) == strings.TrimSpace(truth)
		}
	}
	return &Verifier{
		read:          read,
		equal:         cfg.Equal,
		template:      cfg.Template,
		source:        cfg.Source,
		strategy:      cfg.Strategy,
		queueSize:     cfg.QueueSize,
		sampleCap:     cfg.SampleCap,
		verifyTimeout: cfg.VerifyTimeout,
		queue:         make(chan ReadObservation, cfg.QueueSize),
	}
}

// Start launches the background worker. It returns immediately; the worker stops when stop is cancelled or Stop is called.
func (v *Verifier) Start(stop context.Context) {
	if v == nil {
		return
	}
	v.mu.Lock()
	if v.started {
		v.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(stop)
	v.stopCtx = ctx
	v.cancel = cancel
	v.started = true
	v.mu.Unlock()

	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		v.run(ctx)
	}()
}

// Stop cancels the worker and waits for it to exit. Idempotent.
func (v *Verifier) Stop() {
	if v == nil {
		return
	}
	v.mu.Lock()
	cancel := v.cancel
	v.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	v.wg.Wait()
}

// Observe enqueues a cache hit for out-of-band verification. It never blocks the caller.
func (v *Verifier) Observe(obs ReadObservation) {
	if v == nil || v.read == nil || !Enabled(v.strategy) || strings.TrimSpace(obs.Key) == "" {
		return
	}
	select {
	case v.queue <- obs:
	default:
		verifyDroppedTotal.WithLabelValues(v.source, v.template).Inc()
	}
}

// Samples returns a newest-first copy of the stale-read ring.
func (v *Verifier) Samples() []Sample {
	if v == nil {
		return nil
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]Sample, len(v.samples))
	copy(out, v.samples)
	return out
}

type samplesResponse struct {
	Samples        []Sample        `json:"samples"`
	ReplicaSamples []ReplicaSample `json:"replicaSamples"`
}

// SamplesHandler serves GET JSON with cache stale rows and optional read-router replica samples.
func SamplesHandler(v *Verifier, replicaSrc ReplicaSamplesSource) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := samplesResponse{Samples: []Sample{}, ReplicaSamples: []ReplicaSample{}}
		if v != nil {
			resp.Samples = v.Samples()
		}
		if replicaSrc != nil {
			resp.ReplicaSamples = replicaSrc.ReplicaSamples()
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
}

func (v *Verifier) run(stop context.Context) {
	for {
		select {
		case <-stop.Done():
			return
		case obs, ok := <-v.queue:
			if !ok {
				return
			}
			v.verifyOne(stop, obs)
		}
	}
}

func (v *Verifier) verifyOne(stop context.Context, obs ReadObservation) {
	ctx, cancel := context.WithTimeout(stop, v.verifyTimeout)
	defer cancel()

	truth, err := v.read(ctx, obs.Key)
	if err != nil {
		if ctx.Err() == nil {
			verifyErrorsTotal.WithLabelValues(v.source, v.template).Inc()
		}
		return
	}

	readsCheckedTotal.WithLabelValues(v.source, v.template).Inc()

	if v.equal(obs.Served, truth) {
		return
	}

	readStaleTotal.WithLabelValues(v.source, v.template).Inc()

	served := truncateRunes(obs.Served, maxEvidenceRunes)
	truthVal := truncateRunes(truth, maxEvidenceRunes)
	sample := Sample{
		Key:       obs.Key,
		Source:    obs.Source,
		Served:    served,
		Truth:     truthVal,
		RequestID: obs.RequestID,
		At:        time.Now().UTC(),
	}
	v.prependSample(sample)

	slog.Info("consistency stale",
		"op", "consistency_stale",
		"request_id", obs.RequestID,
		"key", obs.Key,
		"served", served,
		"truth", truthVal,
		"source", obs.Source,
		"template", v.template,
	)
}

func (v *Verifier) prependSample(s Sample) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.samples = append([]Sample{s}, v.samples...)
	if len(v.samples) > v.sampleCap {
		v.samples = v.samples[:v.sampleCap]
	}
}

func truncateRunes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}
