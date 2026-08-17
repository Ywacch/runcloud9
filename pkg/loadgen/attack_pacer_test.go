package loadgen

import (
	"testing"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

func TestRatePacer_steadyWait(t *testing.T) {
	t.Parallel()
	p := newRatePacer(10)
	wait, stop := p.Pace(time.Second, 10)
	if stop {
		t.Fatal("unexpected stop")
	}
	if wait <= 0 {
		t.Fatalf("at expected hits, want positive wait, got %v", wait)
	}
	if wait > 150*time.Millisecond {
		t.Fatalf("wait too long for 10 rps: %v", wait)
	}
}

func TestRatePacer_increaseDoesNotBurst(t *testing.T) {
	t.Parallel()
	p := newRatePacer(1)
	_, _ = p.Pace(10*time.Second, 10)
	p.SetTarget(100)
	wait, stop := p.Pace(10*time.Second, 10)
	if stop {
		t.Fatal("unexpected stop")
	}
	if wait == 0 {
		t.Fatal("rate increase without rebase would burst (wait=0); expected short wait after rebase")
	}
	if wait > 50*time.Millisecond {
		t.Fatalf("after rebase to 100 rps, next hit should wait ~10ms, got %v", wait)
	}
}

func TestLiveTargeter_swapVisible(t *testing.T) {
	t.Parallel()
	lt := &liveTargeter{}
	lt.Swap(&targeterState{next: func(tg *vegeta.Target) error {
		tg.URL = "http://a"
		return nil
	}})
	var tgt vegeta.Target
	if err := lt.Targeter()(&tgt); err != nil {
		t.Fatal(err)
	}
	if tgt.URL != "http://a" {
		t.Fatalf("got %q want a", tgt.URL)
	}
	lt.Swap(&targeterState{next: func(tg *vegeta.Target) error {
		tg.URL = "http://b"
		return nil
	}})
	if err := lt.Targeter()(&tgt); err != nil {
		t.Fatal(err)
	}
	if tgt.URL != "http://b" {
		t.Fatalf("got %q want b", tgt.URL)
	}
}

func TestRatePacer_RateMatchesTarget(t *testing.T) {
	t.Parallel()
	p := newRatePacer(25)
	if got := p.Rate(0); got != 25 {
		t.Fatalf("Rate: got %v want 25", got)
	}
	p.SetTarget(80)
	if got := p.Rate(time.Second); got != 80 {
		t.Fatalf("Rate after set: got %v want 80", got)
	}
}

func TestAttackerRPSBudget_phaseAndHeadroom(t *testing.T) {
	t.Parallel()
	phases := []Phase{{RpsMultiplier: 4}}
	got := attackerRPSBudget(25, phases)
	if got < 100 {
		t.Fatalf("budget must cover 4x phase and 4x live headroom: got %d", got)
	}
}

func TestAttackControl_setBaseRPS(t *testing.T) {
	t.Parallel()
	c := NewAttackControl(10)
	if c.BaseRPS() != 10 {
		t.Fatalf("got %d", c.BaseRPS())
	}
	c.SetBaseRPS(40)
	if c.BaseRPS() != 40 {
		t.Fatalf("got %d", c.BaseRPS())
	}
	c.SetBaseRPS(0)
	if c.BaseRPS() != 1 {
		t.Fatalf("zero clamps to 1, got %d", c.BaseRPS())
	}
}

func TestValidateLiveRPS(t *testing.T) {
	t.Parallel()
	if err := ValidateLiveRPS(0); err == nil {
		t.Fatal("expected error for 0")
	}
	if err := ValidateLiveRPS(5001); err == nil {
		t.Fatal("expected error for 5001")
	}
	if err := ValidateLiveRPS(25); err != nil {
		t.Fatal(err)
	}
}
