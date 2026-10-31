package timeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
)

func TestInstrumented_RecordsOps(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	base := timeline.NewMemory(100)
	s := timeline.Instrumented(base, reg)

	ctx := context.Background()
	when := time.Unix(1_700_001_000, 0).UTC()
	if err := s.Append(ctx, []int64{1}, timeline.Item{PostID: 1, CreatedAt: when}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := s.Range(ctx, 1, 5); err != nil {
		t.Fatalf("range: %v", err)
	}
	if err := s.Trim(ctx, 1, 1); err != nil {
		t.Fatalf("trim: %v", err)
	}

	if n := testutil.CollectAndCount(reg, timeline.MetricAppendTotal); n != 1 {
		t.Fatalf("%s count: want 1, got %d", timeline.MetricAppendTotal, n)
	}
	if n := testutil.CollectAndCount(reg, timeline.MetricAppendDuration); n != 1 {
		t.Fatalf("%s count: want 1, got %d", timeline.MetricAppendDuration, n)
	}
	if n := testutil.CollectAndCount(reg, timeline.MetricRangeDuration); n != 1 {
		t.Fatalf("%s count: want 1, got %d", timeline.MetricRangeDuration, n)
	}
	if n := testutil.CollectAndCount(reg, timeline.MetricTrimDuration); n != 1 {
		t.Fatalf("%s count: want 1, got %d", timeline.MetricTrimDuration, n)
	}
}

func TestInstrumented_RecordsOpError(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	s := timeline.Instrumented(timeline.NewMemory(10), reg)
	_, err := s.Range(context.Background(), 1, 0)
	if err == nil {
		t.Fatal("want range error")
	}
	if n := testutil.CollectAndCount(reg, timeline.MetricOpErrorsTotal); n != 1 {
		t.Fatalf("%s count: want 1, got %d", timeline.MetricOpErrorsTotal, n)
	}
}
