package kvcachetest

import (
	"context"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/kvcache"
)

type entry struct {
	value   []byte
	expires time.Time
}

// Fake is an in-memory Cache for conformance tests.
type Fake struct {
	mu    sync.Mutex
	now   func() time.Time
	data  map[string]entry
	locks map[string]time.Time
}

// NewFake returns a Fake that uses time.Now unless now is set.
func NewFake(now func() time.Time) *Fake {
	if now == nil {
		now = time.Now
	}
	return &Fake{
		now:   now,
		data:  make(map[string]entry),
		locks: make(map[string]time.Time),
	}
}

// Engine returns "fake".
func (f *Fake) Engine() string {
	return "fake"
}

func (f *Fake) Get(_ context.Context, key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evictExpiredLocked()
	e, ok := f.data[key]
	if !ok {
		return nil, false, nil
	}
	if !e.expires.IsZero() && !e.expires.After(f.now()) {
		delete(f.data, key)
		return nil, false, nil
	}
	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true, nil
}

func (f *Fake) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evictExpiredLocked()
	cp := make([]byte, len(value))
	copy(cp, value)
	var expires time.Time
	if ttl > 0 {
		expires = f.now().Add(ttl)
	}
	f.data[key] = entry{value: cp, expires: expires}
	return nil
}

func (f *Fake) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, key)
	return nil
}

func (f *Fake) Lock(_ context.Context, key string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evictLocksLocked()
	if exp, ok := f.locks[key]; ok && exp.After(f.now()) {
		return false, nil
	}
	if ttl <= 0 {
		ttl = time.Second
	}
	f.locks[key] = f.now().Add(ttl)
	return true, nil
}

func (f *Fake) evictExpiredLocked() {
	now := f.now()
	for k, e := range f.data {
		if !e.expires.IsZero() && !e.expires.After(now) {
			delete(f.data, k)
		}
	}
}

func (f *Fake) evictLocksLocked() {
	now := f.now()
	for k, exp := range f.locks {
		if !exp.After(now) {
			delete(f.locks, k)
		}
	}
}

var _ kvcache.Cache = (*Fake)(nil)
