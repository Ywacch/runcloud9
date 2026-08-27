package metrics

import "testing"

func TestPgEngineTotalsStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		prev pgEngineTotals
		cur  pgEngineTotals
		want bool
	}{
		{
			name: "seedWriteAndReadUnchanged",
			prev: pgEngineTotals{read: 2_000_000, write: 1_000_000, readOK: true, writeOK: true},
			cur:  pgEngineTotals{read: 2_000_000, write: 1_000_000, readOK: true, writeOK: true},
			want: true,
		},
		{
			name: "writeStillZero",
			prev: pgEngineTotals{read: 0, write: 0, readOK: true, writeOK: true},
			cur:  pgEngineTotals{read: 0, write: 0, readOK: true, writeOK: true},
			want: false,
		},
		{
			name: "writeStillClimbing",
			prev: pgEngineTotals{read: 1_000_000, write: 500_000, readOK: true, writeOK: true},
			cur:  pgEngineTotals{read: 1_000_000, write: 1_000_000, readOK: true, writeOK: true},
			want: false,
		},
		{
			name: "readStillClimbing",
			prev: pgEngineTotals{read: 1_000_000, write: 1_000_000, readOK: true, writeOK: true},
			cur:  pgEngineTotals{read: 2_000_000, write: 1_000_000, readOK: true, writeOK: true},
			want: false,
		},
		{
			name: "writeMissing",
			prev: pgEngineTotals{read: 0, write: 0, readOK: true, writeOK: false},
			cur:  pgEngineTotals{read: 0, write: 1_000_000, readOK: true, writeOK: true},
			want: false,
		},
		{
			name: "readMissing",
			prev: pgEngineTotals{read: 0, write: 1_000_000, readOK: false, writeOK: true},
			cur:  pgEngineTotals{read: 0, write: 1_000_000, readOK: false, writeOK: true},
			want: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := pgEngineTotalsStable(tc.prev, tc.cur); got != tc.want {
				t.Fatalf("pgEngineTotalsStable: want %v, got %v", tc.want, got)
			}
		})
	}
}
