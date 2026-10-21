// Package vu runs virtual users: closed-loop scripted traffic (post, then reply)
// beside Vegeta's open-loop requests per second.
//
// Behaviors own the logic; templates supply HTTP details as named actions resolved
// before the loadgen pod starts. This package imports nothing from pkg/loadgen.
package vu

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"time"
)

// MaxVUs caps total virtual users across all behaviors in one run.
const MaxVUs = 50

// Doer is the HTTP client seam. *http.Client satisfies it.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// VU is one virtual user's state. A VU is used by one goroutine only.
type VU struct {
	ID            int
	UserID        string // identity pool value; also the X-C9-Client-ID seed (S-3)
	ApplyIdentity func(h http.Header, seed string)
	BaseURL       string
	Actions       map[string]Action
	Client        Doer
	RNG           *rand.Rand // per VU, seeded from (runSeed, ID)
	Now           func() time.Time
	Sleep         func(ctx context.Context, d time.Duration) error
	Stats         *Recorder
	Shared        *Shared

	seq int64
}

// Shared holds cross-VU state for one run.
type Shared struct {
	Roots *RootFeed
}

// Behavior is one closed-loop journey. Run returns when ctx is done.
type Behavior interface {
	Name() string
	Run(ctx context.Context, vu *VU) error
}

// Factory builds a behavior from params, applying defaults and checking ranges.
type Factory func(params json.RawMessage) (Behavior, error)

// BehaviorSpec is one registry entry.
type BehaviorSpec struct {
	New      Factory
	Requires []string // action names the template must declare
}

// Registry maps behavior names to specs.
type Registry map[string]BehaviorSpec

// Config selects closed-loop behaviors for the VU lane. Nil means no VU lane.
type Config struct {
	Behaviors []BehaviorConfig `json:"behaviors"`
	Seed      int64            `json:"seed,omitempty"` // 0 means the runner picks one and reports it
}

// BehaviorConfig runs Count VUs of one behavior.
type BehaviorConfig struct {
	Name   string          `json:"name"`
	Count  int             `json:"count"`
	Params json.RawMessage `json:"params,omitempty"`
}

func thinkWithJitter(ctx context.Context, vu *VU, base time.Duration) error {
	if base <= 0 {
		return ctx.Err()
	}
	jitter := 0.8 + 0.4*vu.RNG.Float64()
	return vu.Sleep(ctx, time.Duration(float64(base)*jitter))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
