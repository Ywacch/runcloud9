package metrics

import (
	"strconv"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/chaos"
	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestFailoverWriteLossEvent(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)

	tests := []struct {
		name            string
		replicationMode string
		syncCommitLevel string
		scenario        string
		preFence        string
		promoted        string
		promotedPod     string
		syncState       string
		wantEmit        bool
		wantBytes       int64
	}{
		{
			name:            "semi-sync remote_apply mostCurrent zero loss",
			replicationMode: "semi-sync",
			syncCommitLevel: "remote_apply",
			scenario:        chaos.ScenarioPromoteReplica,
			preFence:        "0/2000",
			promoted:        "0/2000",
			promotedPod:     "postgres-replica-0",
			syncState:       "sync",
			wantEmit:        false,
		},
		{
			name:            "async mostLagging lossy promotion",
			replicationMode: "async",
			syncCommitLevel: "",
			scenario:        chaos.ScenarioPromoteLaggingReplica,
			preFence:        "0/1000",
			promoted:        "0/2000",
			promotedPod:     "postgres-replica-1",
			syncState:       "async",
			wantEmit:        true,
			wantBytes:       4096,
		},
		{
			name:            "semi-sync remote_write lagging candidate",
			replicationMode: "semi-sync",
			syncCommitLevel: "remote_write",
			scenario:        chaos.ScenarioPromoteLaggingReplica,
			preFence:        "0/3000",
			promoted:        "0/5000",
			promotedPod:     "postgres-replica-0",
			syncState:       "streaming",
			wantEmit:        true,
			wantBytes:       8192,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			walLost, err := deployer.WalBytesLost(tc.preFence, tc.promoted)
			if err != nil {
				t.Fatalf("WalBytesLost: %v", err)
			}
			if walLost != tc.wantBytes {
				t.Fatalf("WalBytesLost = %d want %d", walLost, tc.wantBytes)
			}

			ev := FailoverWriteLossEvent(
				now,
				tc.scenario,
				tc.promotedPod,
				tc.syncState,
				walLost,
				tc.replicationMode,
				tc.syncCommitLevel,
			)
			if tc.wantEmit {
				if ev == nil {
					t.Fatal("expected invariant event")
				}
				if ev.Type != "invariant" || ev.RuleID != RuleFailoverWriteLoss {
					t.Fatalf("event: %+v", ev)
				}
				if ev.Source != "chaos" || ev.Component != "db" {
					t.Fatalf("source/component: %+v", ev)
				}
				if ev.Detail["scenario"] != tc.scenario {
					t.Fatalf("scenario detail: %q", ev.Detail["scenario"])
				}
				if ev.Detail["promotedPod"] != tc.promotedPod {
					t.Fatalf("promotedPod detail: %q", ev.Detail["promotedPod"])
				}
				if ev.Detail["syncState"] != tc.syncState {
					t.Fatalf("syncState detail: %q", ev.Detail["syncState"])
				}
				if ev.Detail["walBytesLost"] != strconv.FormatInt(tc.wantBytes, 10) {
					t.Fatalf("walBytesLost detail: %q", ev.Detail["walBytesLost"])
				}
				if ev.Detail["replicationMode"] != tc.replicationMode {
					t.Fatalf("replicationMode detail: %q", ev.Detail["replicationMode"])
				}
				if ev.Detail["syncCommitLevel"] != tc.syncCommitLevel {
					t.Fatalf("syncCommitLevel detail: %q", ev.Detail["syncCommitLevel"])
				}
				return
			}
			if ev != nil {
				t.Fatalf("expected nil event, got %+v", ev)
			}
		})
	}
}
