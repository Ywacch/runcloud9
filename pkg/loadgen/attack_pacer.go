package loadgen

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

var errNoLiveTargeter = errors.New("loadgen: no live targeter")

// AttackControl is the live handle for an in-flight attack. SetBaseRPS changes
// the user-selected base rate; SetBaseDistribution changes base endpoint weights.
// The attack loop applies the current phase multiplier and rebuilds the targeter
// when the mix generation changes.
type AttackControl struct {
	baseRPS  atomic.Uint64
	baseDist atomic.Pointer[map[string]int]
	mixGen   atomic.Uint64
}

// NewAttackControl returns a control seeded with baseRPS (minimum 1).
func NewAttackControl(baseRPS int) *AttackControl {
	c := &AttackControl{}
	c.SetBaseRPS(baseRPS)
	return c
}

// SetBaseRPS stores the user-selected base RPS (minimum 1).
func (c *AttackControl) SetBaseRPS(rps int) {
	if c == nil {
		return
	}
	if rps < 1 {
		rps = 1
	}
	c.baseRPS.Store(uint64(rps))
}

// BaseRPS returns the user-selected base RPS.
func (c *AttackControl) BaseRPS() int {
	if c == nil {
		return 1
	}
	n := int(c.baseRPS.Load())
	if n < 1 {
		return 1
	}
	return n
}

// SetBaseDistribution stores a copy of the user-selected base endpoint weights
// and bumps the mix generation so the attack loop rebuilds the live targeter.
func (c *AttackControl) SetBaseDistribution(dist map[string]int) {
	if c == nil {
		return
	}
	copied := copyIntMap(dist)
	c.baseDist.Store(&copied)
	c.mixGen.Add(1)
}

// BaseDistribution returns a copy of the user-selected base endpoint weights.
func (c *AttackControl) BaseDistribution() map[string]int {
	if c == nil {
		return map[string]int{}
	}
	p := c.baseDist.Load()
	if p == nil {
		return map[string]int{}
	}
	return copyIntMap(*p)
}

// MixGeneration returns a monotonic counter bumped on each SetBaseDistribution.
func (c *AttackControl) MixGeneration() uint64 {
	if c == nil {
		return 0
	}
	return c.mixGen.Load()
}

// ratePacer implements vegeta.Pacer with a live-updatable target RPS.
// When the target changes it rebases against elapsed/hits so the rate
// change does not produce a catch-up burst or lull.
type ratePacer struct {
	target atomic.Uint64

	mu       sync.Mutex
	curRate  float64
	baseEl   time.Duration
	baseHits float64
}

func newRatePacer(rps int) *ratePacer {
	p := &ratePacer{}
	p.SetTarget(rps)
	return p
}

// SetTarget updates the instantaneous hits-per-second (minimum 1).
func (p *ratePacer) SetTarget(rps int) {
	if rps < 1 {
		rps = 1
	}
	p.target.Store(uint64(rps))
}

// Target returns the current effective RPS.
func (p *ratePacer) Target() int {
	n := int(p.target.Load())
	if n < 1 {
		return 1
	}
	return n
}

// Rate implements vegeta.Pacer.
func (p *ratePacer) Rate(_ time.Duration) float64 {
	return float64(p.Target())
}

// Pace implements vegeta.Pacer.
func (p *ratePacer) Pace(elapsed time.Duration, hits uint64) (time.Duration, bool) {
	t := float64(p.target.Load())
	if t < 1 {
		t = 1
	}

	p.mu.Lock()
	if t != p.curRate {
		p.baseEl = elapsed
		p.baseHits = float64(hits)
		p.curRate = t
	}
	rate := p.curRate
	baseEl := p.baseEl
	baseHits := p.baseHits
	p.mu.Unlock()

	if rate <= 0 {
		return 0, false
	}
	dt := (elapsed - baseEl).Seconds()
	if dt < 0 {
		dt = 0
	}
	expected := baseHits + rate*dt
	if hits == 0 || float64(hits) < expected {
		return 0, false
	}
	next := time.Duration((float64(hits+1)-baseHits)/rate*float64(time.Second)) + baseEl
	if w := next - elapsed; w > 0 {
		return w, false
	}
	return 0, false
}

type targeterState struct {
	next vegeta.Targeter
	dist map[string]int
}

func (s *targeterState) fill(t *vegeta.Target) error {
	if s == nil || s.next == nil {
		return errNoLiveTargeter
	}
	return s.next(t)
}

// liveTargeter swaps the vegeta targeter used by in-flight workers without
// stopping the attack. Each hit dereferences the current plan.
type liveTargeter struct {
	cur atomic.Pointer[targeterState]
}

func (lt *liveTargeter) Swap(s *targeterState) {
	if lt == nil || s == nil {
		return
	}
	lt.cur.Store(s)
}

// Targeter returns a vegeta.Targeter that always uses the latest swapped state.
func (lt *liveTargeter) Targeter() vegeta.Targeter {
	return func(t *vegeta.Target) error {
		return lt.cur.Load().fill(t)
	}
}

func attackerRPSBudget(base int, phases []Phase) int {
	if base < 1 {
		base = 1
	}
	max := base
	for _, p := range phases {
		if e := EffectiveRPS(base, p); e > max {
			max = e
		}
	}
	if scaled := base * 4; scaled > max {
		max = scaled
	}
	return max
}
