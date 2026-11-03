package kvcache

import (
	"testing"
	"time"
)

func TestFailGate_skipsAfterThreeFailures(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	g := NewFailGate(func() time.Time { return now })
	if !g.Allow() {
		t.Fatal("gate should allow the first call")
	}
	g.Fail()
	g.Fail()
	if !g.Allow() {
		t.Fatal("two failures should still allow a call")
	}
	g.Fail()
	if g.Allow() {
		t.Fatal("third failure should skip")
	}
	now = now.Add(3 * time.Second)
	if !g.Allow() {
		t.Fatal("skip window should end after 3s")
	}
	g.OK()
	g.Fail()
	if !g.Allow() {
		t.Fatal("one failure after OK should allow a call")
	}
}
