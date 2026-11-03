package kvcache

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestCacheOpResult(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   string
		hit  bool
		err  error
		want string
	}{
		{name: "get hit", op: "get", hit: true, want: "hit"},
		{name: "get miss", op: "get", want: "miss"},
		{name: "set ok", op: "set", want: "ok"},
		{name: "delete ok", op: "delete", want: "ok"},
		{name: "get error", op: "get", err: errors.New("boom"), want: "error"},
		{name: "deadline", op: "set", err: context.DeadlineExceeded, want: "timeout"},
		{name: "net timeout", op: "get", err: timeoutErr{}, want: "timeout"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := cacheOpResult(tc.op, tc.hit, tc.err); got != tc.want {
				t.Fatalf("cacheOpResult() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestObserveCacheOp_knownLabel(t *testing.T) {
	t.Parallel()
	observeCacheOp("memcached", "get", "hit", time.Now())
	if _, ok := cacheOpObservers[observerKey("memcached", "get", "hit")]; !ok {
		t.Fatal("memcached get hit observer missing")
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}
