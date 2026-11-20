package engines

import (
	"strings"
	"testing"
)

func TestCanCoHost(t *testing.T) {
	t.Parallel()
	c := Default()

	tests := []struct {
		name     string
		engine   string
		jobs     []string
		wantOK   bool
		contains string
	}{
		{
			name:   "redis timeline and work_queue",
			engine: "redis",
			jobs:   []string{"timeline", "work_queue"},
			wantOK: true,
		},
		{
			name:   "redis hot_cache and work_queue",
			engine: "redis",
			jobs:   []string{"hot_cache", "work_queue"},
			wantOK: true,
		},
		{
			name:     "invalid job on redis node",
			engine:   "redis",
			jobs:     []string{"hot_cache", "main_db"},
			wantOK:   false,
			contains: "unknown pair",
		},
		{
			name:   "single job",
			engine: "postgresql",
			jobs:   []string{"main_db"},
			wantOK: true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ok, reason := CanCoHost(c, tc.engine, tc.jobs)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v reason=%q", ok, reason)
			}
			if tc.contains != "" && reason != "" && !strings.Contains(reason, tc.contains) {
				t.Fatalf("reason %q want substring %q", reason, tc.contains)
			}
		})
	}
}

func TestPairCoHost_manifestFlags(t *testing.T) {
	t.Parallel()
	c := Default()
	p, ok := c.Pair("hot_cache", "memcached")
	if !ok {
		t.Fatal("pair")
	}
	if PairCoHost(p) {
		t.Fatal("memcached hot_cache coHost should be false")
	}
	p, ok = c.Pair("work_queue", "rabbitmq")
	if !ok {
		t.Fatal("pair")
	}
	if PairCoHost(p) {
		t.Fatal("rabbitmq work_queue coHost should be false")
	}
}
