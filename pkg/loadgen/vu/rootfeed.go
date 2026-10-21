package vu

import (
	"sync"
	"time"
)

const rootFeedCap = 256

// RootPost is a root created by a VU.
type RootPost struct {
	ID       string
	PostedAt time.Time
}

type rootEntry struct {
	post    RootPost
	replied bool
}

// RootFeed is a bounded ring of recent roots shared by all VUs in a run.
type RootFeed struct {
	mu   sync.Mutex
	ring [rootFeedCap]rootEntry
	head int
	n    int
}

// NewRootFeed returns an empty feed.
func NewRootFeed() *RootFeed {
	return &RootFeed{}
}

// Publish records a root, dropping the oldest when full.
func (f *RootFeed) Publish(p RootPost) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ring[f.head] = rootEntry{post: p}
	f.head = (f.head + 1) % rootFeedCap
	if f.n < rootFeedCap {
		f.n++
	}
}

// TakeForReply returns the newest unreplied root and marks it replied.
func (f *RootFeed) TakeForReply() (RootPost, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 0; i < f.n; i++ {
		idx := (f.head - 1 - i + rootFeedCap) % rootFeedCap
		if !f.ring[idx].replied {
			f.ring[idx].replied = true
			return f.ring[idx].post, true
		}
	}
	return RootPost{}, false
}

// Recent returns roots no older than maxAge at now, newest first.
func (f *RootFeed) Recent(maxAge time.Duration, now time.Time) []RootPost {
	f.mu.Lock()
	defer f.mu.Unlock()
	cutoff := now.Add(-maxAge)
	var out []RootPost
	for i := 0; i < f.n; i++ {
		idx := (f.head - 1 - i + rootFeedCap) % rootFeedCap
		p := f.ring[idx].post
		if p.PostedAt.Before(cutoff) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// Len reports how many roots the feed holds.
func (f *RootFeed) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}
