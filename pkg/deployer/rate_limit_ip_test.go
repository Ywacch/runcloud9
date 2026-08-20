package deployer

import "testing"

func TestRateLimitPerIP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  map[string]any
		want int
	}{
		{name: "missing", cfg: nil, want: 0},
		{name: "zero", cfg: map[string]any{"rateLimitPerIp": 0}, want: 0},
		{name: "negative", cfg: map[string]any{"rateLimitPerIp": -1}, want: 0},
		{name: "invalid string", cfg: map[string]any{"rateLimitPerIp": "nope"}, want: 0},
		{name: "int five", cfg: map[string]any{"rateLimitPerIp": 5}, want: 5},
		{name: "float five", cfg: map[string]any{"rateLimitPerIp": 5.0}, want: 5},
		{name: "string five", cfg: map[string]any{"rateLimitPerIp": "5"}, want: 5},
		{name: "clamp over max", cfg: map[string]any{"rateLimitPerIp": 200}, want: 100},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := rateLimitPerIP(tc.cfg); got != tc.want {
				t.Fatalf("rateLimitPerIP() = %d want %d", got, tc.want)
			}
		})
	}
}
