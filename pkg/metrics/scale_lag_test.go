package metrics

import "testing"

func TestNon2xxCount(t *testing.T) {
	t.Parallel()
	got := non2xxCount(map[string]int{"200": 10, "500": 2, "0": 1, "404": 0})
	if got != 3 {
		t.Fatalf("got %d want 3", got)
	}
}

func TestNextScaleLagErrors_AccumulatesWhileLagging(t *testing.T) {
	t.Parallel()
	got := nextScaleLagErrors(0, true, nil, map[string]int{"200": 10, "500": 2, "0": 1})
	if got != 3 {
		t.Fatalf("got %d want 3", got)
	}
}

func TestNextScaleLagErrors_ResetsWhenCaughtUp(t *testing.T) {
	t.Parallel()
	got := nextScaleLagErrors(3, false, map[string]int{"500": 3}, map[string]int{"500": 5})
	if got != 0 {
		t.Fatalf("got %d want 0", got)
	}
}

func TestNextScaleLagErrors_RestartUsesNewTotal(t *testing.T) {
	t.Parallel()
	prevCodes := map[string]int{"500": 5}
	curCodes := map[string]int{"500": 1}
	got := nextScaleLagErrors(4, true, prevCodes, curCodes)
	if got != 5 {
		t.Fatalf("got %d want 5", got)
	}
}
