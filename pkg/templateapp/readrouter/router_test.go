package readrouter

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestParseLSN_roundTripAndMalformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    uint64
		wantErr bool
	}{
		{name: "zero", in: "0/0", want: 0},
		{name: "low nibble", in: "0/1A2B", want: 0x1a2b},
		{name: "high segment", in: "1/0", want: 1 << 32},
		{name: "combined", in: "2/3", want: (2 << 32) | 3},
		{name: "postgres sample", in: "0/16BC1234", want: 0x16bc1234},
		{name: "malformed missing slash", in: "0123", wantErr: true},
		{name: "malformed hex", in: "0/ZZ", wantErr: true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseLSN(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseLSN(%q) expected error", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLSN(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("parseLSN(%q) = %x want %x", tc.in, got, tc.want)
			}
			tok := lsnToken(got)
			back, err := parseLSN(tok.String())
			if err != nil {
				t.Fatalf("round-trip parse: %v", err)
			}
			if back != got {
				t.Fatalf("round-trip = %x want %x", back, got)
			}
		})
	}
}

func TestLSNToken_GTE(t *testing.T) {
	t.Parallel()

	a := lsnToken(10)
	b := lsnToken(20)
	if a.GTE(b) {
		t.Fatal("10 should not GTE 20")
	}
	if !b.GTE(a) {
		t.Fatal("20 should GTE 10")
	}
	if !a.GTE(lsnToken(10)) {
		t.Fatal("10 should GTE 10")
	}
}

func TestPostgresEngine_ParseToken(t *testing.T) {
	t.Parallel()

	eng := PostgresEngine{}
	tok, err := eng.ParseToken("0/10")
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if tok.String() != "0/10" {
		t.Fatalf("token string: got %q", tok.String())
	}
	if eng.WriteTokenExpr() != "pg_current_wal_lsn()::text" {
		t.Fatalf("WriteTokenExpr mismatch")
	}
	if eng.ReadTokenExpr() != "pg_last_wal_replay_lsn()::text" {
		t.Fatalf("ReadTokenExpr mismatch")
	}
}

func TestReadRouter_RouteRead_policies(t *testing.T) {
	t.Parallel()

	eng := PostgresEngine{}
	writeTok, err := eng.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	readTok, err := eng.ParseToken("0/50")
	if err != nil {
		t.Fatal(err)
	}
	caughtUp, err := eng.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		pol  RoutingPolicy
		seed func(r *ReadRouter)
		want Route
	}{
		{name: "leader-only", pol: PolicyLeaderOnly, want: RouteLeader},
		{name: "replica", pol: PolicyReplica, want: RouteReplica},
		{name: "monotonic", pol: PolicyMonotonic, want: RouteReplica},
		{name: "sticky", pol: PolicySticky, want: RouteReplica},
		{
			name: "ryw without write",
			pol:  PolicyRYW,
			want: RouteReplica,
		},
		{
			name: "ryw stale replay",
			pol:  PolicyRYW,
			seed: func(r *ReadRouter) {
				r.AfterWrite("client-a", writeTok)
				r.RecordRead("client-a", readTok)
			},
			want: RouteLeader,
		},
		{
			name: "ryw caught up",
			pol:  PolicyRYW,
			seed: func(r *ReadRouter) {
				r.AfterWrite("client-a", writeTok)
				r.RecordRead("client-a", caughtUp)
			},
			want: RouteReplica,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewReadRouter(nil, nil, eng, tc.pol)
			if tc.seed != nil {
				tc.seed(r)
			}
			if got := r.RouteRead("client-a", tc.pol); got != tc.want {
				t.Fatalf("RouteRead = %v want %v", got, tc.want)
			}
		})
	}
}

func TestReadRouter_RecordRead_violations(t *testing.T) {
	eng := PostgresEngine{}
	writeTok, _ := eng.ParseToken("0/100")
	oldRead, _ := eng.ParseToken("0/40")
	newRead, _ := eng.ParseToken("0/60")
	staleRead, _ := eng.ParseToken("0/80")

	t.Run("ryw violation", func(t *testing.T) {
		r := NewReadRouter(nil, nil, eng, PolicyRYW)
		r.AfterWrite("c1", writeTok)
		ryw, mono := r.RecordRead("c1", staleRead)
		if !ryw || mono {
			t.Fatalf("ryw=%v mono=%v want ryw only", ryw, mono)
		}
	})

	t.Run("monotonic violation", func(t *testing.T) {
		r := NewReadRouter(nil, nil, eng, PolicyMonotonic)
		r.RecordRead("c1", newRead)
		ryw, mono := r.RecordRead("c1", oldRead)
		if ryw || !mono {
			t.Fatalf("ryw=%v mono=%v want mono only", ryw, mono)
		}
	})

	t.Run("ryw+monotonic both", func(t *testing.T) {
		r := NewReadRouter(nil, nil, eng, PolicyRYWMonotonic)
		r.AfterWrite("c1", writeTok)
		r.RecordRead("c1", newRead)
		ryw, mono := r.RecordRead("c1", oldRead)
		if !ryw || !mono {
			t.Fatalf("ryw=%v mono=%v want both", ryw, mono)
		}
	})

	t.Run("replica policy stale read counter", func(t *testing.T) {
		r := NewReadRouter(nil, nil, eng, PolicyReplica)
		r.AfterWrite("c1", writeTok)
		staleBefore := testutil.ToFloat64(replicaStaleReadsTotal)
		rywBefore := testutil.ToFloat64(rywViolationsTotal)
		ryw, mono := r.RecordRead("c1", staleRead)
		if ryw || mono {
			t.Fatalf("ryw=%v mono=%v want neither", ryw, mono)
		}
		if testutil.ToFloat64(replicaStaleReadsTotal)-staleBefore != 1 {
			t.Fatalf("expected replica stale counter increment")
		}
		if testutil.ToFloat64(rywViolationsTotal)-rywBefore != 0 {
			t.Fatalf("ryw violations must not increment under replica policy")
		}
	})
}

func TestCompileAssertions(t *testing.T) {
	t.Parallel()
	var _ EnforcementPoint = (*ReadRouter)(nil)
	var _ Engine = PostgresEngine{}
}

func TestNewReadRouter_skipsInvalidReplicaPools(t *testing.T) {
	t.Parallel()
	r := NewReadRouter(nil, []ReplicaPool{
		{Host: "postgres-replica-0"},
		{Host: "  "},
	}, PostgresEngine{}, PolicyReplica)
	if len(r.readPools) != 0 {
		t.Fatalf("invalid entries should be skipped, got %d read pools", len(r.readPools))
	}
}
