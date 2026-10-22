package phenomena

import (
	"testing"
	"time"
)

func TestBuildReplyBeforeParent(t *testing.T) {
	t.Parallel()

	exampleAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	example := Example{
		RequestID: "req-1",
		At:        exampleAt,
		Detail:    map[string]string{"rootId": "7"},
	}

	cases := []struct {
		name string
		in   ReplyBeforeParentInput
		want *Entry
	}{
		{
			name: "no reads gives nil",
			in:   ReplyBeforeParentInput{},
			want: nil,
		},
		{
			name: "app only",
			in: ReplyBeforeParentInput{
				AppReads:        100,
				AppOrphanReads:  4,
				ReadConsistency: "replica",
				DelaysMs:        []int{0, 1500},
			},
			want: &Entry{
				Phenomenon: phenomenonReplyBeforeParent,
				Fidelity:   "simulated",
				Cost:       Cost{ReadsAffected: 4},
				BaseRate: BaseRate{
					PerThousand: 40,
					Denominator: "all thread reads",
					Reads:       100,
				},
				Expected: &RateRange{MinPerThousand: 150, MaxPerThousand: 400},
				Sources:  []string{"app"},
			},
		},
		{
			name: "app plus observer",
			in: ReplyBeforeParentInput{
				AppReads:                 200,
				AppOrphanReads:           10,
				ObserverWindowReads:      50,
				ObserverViolations:       12,
				ObserverExamples:         []Example{example},
				ReadConsistency:          "replica",
				DelaysMs:                 []int{0, 1500},
				DetectionMayBeIncomplete: true,
			},
			want: &Entry{
				Phenomenon: phenomenonReplyBeforeParent,
				Fidelity:   "simulated",
				Cost:       Cost{ReadsAffected: 12},
				BaseRate: BaseRate{
					PerThousand: 240,
					Denominator: "observer thread reads in window",
					Reads:       50,
				},
				Expected:                 &RateRange{MinPerThousand: 150, MaxPerThousand: 400},
				Sources:                  []string{"app", "observer"},
				Examples:                 []Example{example},
				DetectionMayBeIncomplete: true,
			},
		},
		{
			name: "equal delays expected 0-5",
			in: ReplyBeforeParentInput{
				AppReads:       1000,
				AppOrphanReads: 2,
				DelaysMs:       []int{1500, 1500},
			},
			want: &Entry{
				Phenomenon: phenomenonReplyBeforeParent,
				Fidelity:   "simulated",
				Cost:       Cost{ReadsAffected: 2},
				BaseRate: BaseRate{
					PerThousand: 2,
					Denominator: "all thread reads",
					Reads:       1000,
				},
				Expected: &RateRange{MinPerThousand: 0, MaxPerThousand: 5},
				Sources:  []string{"app"},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := BuildReplyBeforeParent(tc.in)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("got %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("got nil")
			}
			if got.Phenomenon != tc.want.Phenomenon || got.Fidelity != tc.want.Fidelity {
				t.Fatalf("header: %+v", got)
			}
			if got.Cost != tc.want.Cost {
				t.Fatalf("cost: got %+v want %+v", got.Cost, tc.want.Cost)
			}
			if got.BaseRate != tc.want.BaseRate {
				t.Fatalf("baseRate: got %+v want %+v", got.BaseRate, tc.want.BaseRate)
			}
			if got.Expected == nil || tc.want.Expected == nil || *got.Expected != *tc.want.Expected {
				t.Fatalf("expected: got %+v want %+v", got.Expected, tc.want.Expected)
			}
			if got.DetectionMayBeIncomplete != tc.want.DetectionMayBeIncomplete {
				t.Fatalf("incomplete: %v", got.DetectionMayBeIncomplete)
			}
			if len(got.Sources) != len(tc.want.Sources) {
				t.Fatalf("sources: %v", got.Sources)
			}
			for i, s := range tc.want.Sources {
				if got.Sources[i] != s {
					t.Fatalf("sources: %v", got.Sources)
				}
			}
			if len(got.Examples) != len(tc.want.Examples) {
				t.Fatalf("examples: %+v", got.Examples)
			}
		})
	}
}

func TestExpectedReplyBeforeParent(t *testing.T) {
	t.Parallel()
	r := expectedReplyBeforeParent([]int{0, 1500})
	if r.MinPerThousand != 150 || r.MaxPerThousand != 400 {
		t.Fatalf("stagger band: %+v", r)
	}
	r = expectedReplyBeforeParent([]int{100, 100})
	if r.MinPerThousand != 0 || r.MaxPerThousand != 5 {
		t.Fatalf("uniform: %+v", r)
	}
}
