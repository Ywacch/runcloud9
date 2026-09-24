package api

import (
	"testing"
)

func TestSessionTokenBudget_TrySpend(t *testing.T) {
	t.Parallel()

	b := &SessionTokenBudget{limit: 100, used: make(map[string]int)}
	if !b.TrySpend("sess-1", 40) {
		t.Fatal("first spend should succeed")
	}
	if !b.TrySpend("sess-1", 50) {
		t.Fatal("second spend should succeed")
	}
	if b.TrySpend("sess-1", 20) {
		t.Fatal("third spend should exceed limit")
	}
	if b.Spent("sess-1") != 90 {
		t.Fatalf("spent: %d", b.Spent("sess-1"))
	}
	if b.Remaining("sess-1") != 10 {
		t.Fatalf("remaining: %d", b.Remaining("sess-1"))
	}
}
