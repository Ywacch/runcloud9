package main

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestThreads_OrphanCounter(t *testing.T) {
	t.Parallel()

	replies := []Post{{ID: 2, ReplyToID: intPtr(99)}}
	parents := []Post{{ID: 1}}

	beforeReads := testutil.ToFloat64(socialThreadReadsTotal.WithLabelValues("user"))
	beforeOrphans := testutil.ToFloat64(socialThreadOrphanReadsTotal.WithLabelValues("user"))

	recordThreadReadMetrics("user", replies, parents)

	afterReads := testutil.ToFloat64(socialThreadReadsTotal.WithLabelValues("user"))
	afterOrphans := testutil.ToFloat64(socialThreadOrphanReadsTotal.WithLabelValues("user"))

	if afterReads != beforeReads+1 {
		t.Fatalf("reads counter: before %v after %v", beforeReads, afterReads)
	}
	if afterOrphans != beforeOrphans+1 {
		t.Fatalf("orphan counter: before %v after %v", beforeOrphans, afterOrphans)
	}
}

func TestThreadHasOrphan_noOrphan(t *testing.T) {
	t.Parallel()
	replies := []Post{{ID: 2, ReplyToID: intPtr(1)}}
	parents := []Post{{ID: 1}}
	if threadHasOrphan(replies, parents) {
		t.Fatal("expected no orphan")
	}
}

func intPtr(n int) *int {
	return &n
}
