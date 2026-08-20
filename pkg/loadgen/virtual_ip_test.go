package loadgen

import (
	"strconv"
	"strings"
	"testing"
)

func TestVirtualClientIP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b string
		same bool
	}{
		{name: "empty seed", a: "", b: "", same: true},
		{name: "stable seed", a: "user-42", b: "user-42", same: true},
		{name: "different seeds", a: "1", b: "2", same: false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ipA := virtualClientIP(tc.a)
			ipB := virtualClientIP(tc.b)
			if ipA == "" || ipB == "" {
				t.Fatalf("expected valid addresses, got %q and %q", ipA, ipB)
			}
			if tc.same && ipA != ipB {
				t.Fatalf("expected same IP for %q, got %q vs %q", tc.a, ipA, ipB)
			}
			if !tc.same && ipA == ipB {
				t.Fatalf("expected different IPs for %q vs %q, both %q", tc.a, tc.b, ipA)
			}
		})
	}
	got := virtualClientIP("user-42")
	if !strings.HasPrefix(got, "198.51.100.") {
		t.Fatalf("prefix: %q", got)
	}
	last := strings.TrimPrefix(got, "198.51.100.")
	n, err := strconv.Atoi(last)
	if err != nil || n < 1 || n > 254 {
		t.Fatalf("octet: %q", got)
	}
	if virtualClientIP("") != "198.51.100.1" {
		t.Fatalf("empty seed: got %q want 198.51.100.1", got)
	}
}
