package hub

import "testing"

func TestEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		env  string
		want bool
	}{
		{"", true},
		{"1", true},
		{"true", true},
		{"on", true},
		{"0", false},
		{"false", false},
		{"no", false},
		{"off", false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.env, func(t *testing.T) {
			t.Parallel()
			if got := Enabled(tc.env); got != tc.want {
				t.Fatalf("Enabled(%q) = %v, want %v", tc.env, got, tc.want)
			}
		})
	}
}
