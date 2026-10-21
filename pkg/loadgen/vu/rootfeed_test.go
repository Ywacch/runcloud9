package vu

import (
	"testing"
	"time"
)

func TestRootFeed_DropOldestAndRecent(t *testing.T) {
	f := NewRootFeed()
	now := time.Unix(1000, 0)
	for i := 0; i < rootFeedCap+5; i++ {
		f.Publish(RootPost{ID: string(rune('a' + i%26)), PostedAt: now.Add(time.Duration(i) * time.Second)})
	}
	if f.Len() != rootFeedCap {
		t.Fatalf("len: got %d want %d", f.Len(), rootFeedCap)
	}
	recent := f.Recent(10*time.Second, now.Add(time.Duration(rootFeedCap+4)*time.Second))
	if len(recent) == 0 {
		t.Fatal("expected recent roots")
	}
}
