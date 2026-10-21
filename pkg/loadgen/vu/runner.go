package vu

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// RunnerDeps configures a Runner.
type RunnerDeps struct {
	BaseURL       string
	Actions       map[string]Action
	UserIDs       []string
	Client        Doer
	ApplyIdentity func(h http.Header, seed string)
	Now           func() time.Time
	Sleep         func(ctx context.Context, d time.Duration) error
}

// Runner runs VU goroutines until Stop.
type Runner struct {
	reg     Registry
	cfg     Config
	deps    RunnerDeps
	shared  Shared
	rec     *Recorder
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// NewRunner validates cfg and deps, then returns a runner ready to Start.
func NewRunner(reg Registry, cfg Config, deps RunnerDeps) (*Runner, error) {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Sleep == nil {
		deps.Sleep = sleepCtx
	}
	if len(deps.UserIDs) == 0 {
		return nil, fmt.Errorf("vu: at least one user id required")
	}
	if err := ValidateConfig(reg, cfg, deps.Actions); err != nil {
		return nil, err
	}
	seed := cfg.Seed
	if seed == 0 {
		var b [8]byte
		if _, err := cryptorand.Read(b[:]); err != nil {
			return nil, fmt.Errorf("vu: seed: %w", err)
		}
		seed = int64(binary.LittleEndian.Uint64(b[:]))
		if seed == 0 {
			seed = 1
		}
	}
	return &Runner{
		reg:    reg,
		cfg:    cfg,
		deps:   deps,
		shared: Shared{Roots: NewRootFeed()},
		rec:    NewRecorder(seed),
	}, nil
}

// Start launches VU goroutines. It is an error to call Start twice.
func (r *Runner) Start(parent context.Context) error {
	if r.started {
		return fmt.Errorf("vu: runner already started")
	}
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	r.started = true

	vuIndex := 0
	for _, bc := range r.cfg.Behaviors {
		spec := r.reg[bc.Name]
		behavior, err := spec.New(bc.Params)
		if err != nil {
			cancel()
			return err
		}
		r.rec.AddVUs(bc.Name, bc.Count)
		for i := 0; i < bc.Count; i++ {
			id := vuIndex
			vuIndex++
			userID := r.deps.UserIDs[id%len(r.deps.UserIDs)]
			vu := &VU{
				ID:            id,
				UserID:        userID,
				ApplyIdentity: r.deps.ApplyIdentity,
				BaseURL:       r.deps.BaseURL,
				Actions:       r.deps.Actions,
				Client:        r.deps.Client,
				RNG:           rand.New(rand.NewSource(r.rec.seed + int64(id))),
				Now:           r.deps.Now,
				Sleep:         r.deps.Sleep,
				Stats:         r.rec,
				Shared:        &r.shared,
			}
			r.wg.Add(1)
			go func(b Behavior, v *VU) {
				defer r.wg.Done()
				_ = b.Run(ctx, v)
			}(behavior, vu)
		}
	}
	return nil
}

// Stop cancels the run and waits for goroutines. Safe to call more than once.
func (r *Runner) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

// Stats returns the latest snapshot.
func (r *Runner) Stats() Stats {
	if r.rec == nil {
		return Stats{Behaviors: map[string]BehaviorStats{}}
	}
	return r.rec.Snapshot()
}
