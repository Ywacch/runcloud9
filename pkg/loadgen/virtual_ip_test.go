package loadgen

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func virtualClientSubnetPrefix(a, b, c byte) string {
	return fmt.Sprintf("%d.%d.%d.", a, b, c)
}

func isVirtualClientIP(ip string) bool {
	for _, sub := range virtualClientSubnets {
		if !strings.HasPrefix(ip, virtualClientSubnetPrefix(sub.a, sub.b, sub.c)) {
			continue
		}
		last := strings.TrimPrefix(ip, virtualClientSubnetPrefix(sub.a, sub.b, sub.c))
		n, err := strconv.Atoi(last)
		if err != nil || n < 1 || n > virtualClientPoolSize {
			return false
		}
		return true
	}
	return false
}

func nginxIPHashMod3(a, b, c byte) int {
	h := 0
	for _, octet := range []byte{a, b, c} {
		h = (h*113 + int(octet)) % 6271
	}
	return h % 3
}

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
			if !isVirtualClientIP(ipA) || !isVirtualClientIP(ipB) {
				t.Fatalf("expected virtual pool address, got %q and %q", ipA, ipB)
			}
			if tc.same && ipA != ipB {
				t.Fatalf("expected same IP for %q, got %q vs %q", tc.a, ipA, ipB)
			}
			if !tc.same && ipA == ipB {
				t.Fatalf("expected different IPs for %q vs %q, both %q", tc.a, tc.b, ipA)
			}
		})
	}
	if virtualClientIP("") != defaultVirtualClientIP {
		t.Fatalf("empty seed: got %q want %q", virtualClientIP(""), defaultVirtualClientIP)
	}
}

func TestVirtualClientIP_coversAllSubnets(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{})
	for i := 0; i < 2000; i++ {
		seed := fmt.Sprintf("seed-%d", i)
		ip := virtualClientIP(seed)
		if !isVirtualClientIP(ip) {
			t.Fatalf("seed %q: invalid ip %q", seed, ip)
		}
		for _, sub := range virtualClientSubnets {
			if strings.HasPrefix(ip, virtualClientSubnetPrefix(sub.a, sub.b, sub.c)) {
				seen[virtualClientSubnetPrefix(sub.a, sub.b, sub.c)] = struct{}{}
				break
			}
		}
	}
	for _, sub := range virtualClientSubnets {
		prefix := virtualClientSubnetPrefix(sub.a, sub.b, sub.c)
		if _, ok := seen[prefix]; !ok {
			t.Fatalf("expected seeds to cover subnet %s", prefix)
		}
	}
}

func TestVirtualClientSubnets_nginxIPHashBuckets(t *testing.T) {
	t.Parallel()
	want := []int{0, 1, 2}
	got := make([]int, len(virtualClientSubnets))
	for i, sub := range virtualClientSubnets {
		got[i] = nginxIPHashMod3(sub.a, sub.b, sub.c)
	}
	for _, bucket := range want {
		found := false
		for _, g := range got {
			if g == bucket {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing nginx ip_hash bucket %d for subnets; got %v", bucket, got)
		}
	}
}
