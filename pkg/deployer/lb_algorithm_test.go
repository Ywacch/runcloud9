package deployer

import "testing"

func TestNormalizeLBAlgorithm(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want string
	}{
		{in: "", want: "round-robin"},
		{in: "round-robin", want: "round-robin"},
		{in: "round_robin", want: "round-robin"},
		{in: "least_conn", want: "least_conn"},
		{in: "least-connections", want: "least_conn"},
		{in: "random", want: "random"},
		{in: "ip_hash", want: "ip_hash"},
		{in: "ip-hash", want: "ip_hash"},
		{in: "iphash", want: "ip_hash"},
		{in: "least_time", want: "least_time"}, // unknown kept for validate reject
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.in+"→"+tc.want, func(t *testing.T) {
			t.Parallel()
			if got := normalizeLBAlgorithm(tc.in); got != tc.want {
				t.Fatalf("normalizeLBAlgorithm(%q) = %q want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestIsLiveLBAlgorithm(t *testing.T) {
	t.Parallel()
	if !isLiveLBAlgorithm("random") || !isLiveLBAlgorithm("least_conn") || !isLiveLBAlgorithm("round-robin") || !isLiveLBAlgorithm("ip_hash") {
		t.Fatal("expected live allowlist membership")
	}
	if isLiveLBAlgorithm("least_time") {
		t.Fatal("Plus methods must not be live")
	}
}
