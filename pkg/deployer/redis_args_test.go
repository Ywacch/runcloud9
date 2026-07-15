package deployer

import (
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNormalizeRedisEvictionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{"allkeys-lru", "allkeys-lru"},
		{"  VOLATILE-LFU  ", "volatile-lfu"},
		{"bogus", "allkeys-lru"},
		{"", "allkeys-lru"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			if got := normalizeRedisEvictionPolicy(tt.in); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestRedisServerArgs(t *testing.T) {
	t.Parallel()

	res := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	wantBytes := res.Limits.Memory().Value() * 90 / 100

	tests := []struct {
		name   string
		config map[string]interface{}
		want   []string
	}{
		{
			name:   "default policy with maxmemory",
			config: map[string]interface{}{},
			want:   []string{"--maxmemory", strconv.FormatInt(wantBytes, 10), "--maxmemory-policy", "allkeys-lru"},
		},
		{
			name:   "canvas evictionPolicy",
			config: map[string]interface{}{"evictionPolicy": "volatile-ttl"},
			want:   []string{"--maxmemory", strconv.FormatInt(wantBytes, 10), "--maxmemory-policy", "volatile-ttl"},
		},
		{
			name:   "unknown policy falls back",
			config: map[string]interface{}{"evictionPolicy": "something-else"},
			want:   []string{"--maxmemory", strconv.FormatInt(wantBytes, 10), "--maxmemory-policy", "allkeys-lru"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := redisServerArgs(tt.config, res)
			if len(got) != len(tt.want) {
				t.Fatalf("len: got %v want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("args[%d]: got %q want %q (full got=%v)", i, got[i], tt.want[i], got)
				}
			}
		})
	}
}

func TestRedisServerArgs_noMemoryLimit(t *testing.T) {
	t.Parallel()
	got := redisServerArgs(map[string]interface{}{"evictionPolicy": "allkeys-lfu"}, corev1.ResourceRequirements{})
	want := []string{"--maxmemory-policy", "allkeys-lfu"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}
