package main

import (
	"testing"
	"time"
)

func TestBucketIndex(t *testing.T) {
	t.Parallel()
	cases := []struct {
		overlap int
		want    int
	}{
		{overlap: 0, want: 0},
		{overlap: 2, want: 1},
		{overlap: 4, want: 2},
		{overlap: 6, want: 3},
	}
	for _, tc := range cases {
		if got := bucketIndex(tc.overlap); got != tc.want {
			t.Fatalf("bucketIndex(%d)=%d want=%d", tc.overlap, got, tc.want)
		}
	}
}

func TestBuildReportOverlapAndSignatures(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	rows := []deploymentRow{
		{
			Namespace: "a",
			Status:    "terminated",
			CreatedAt: now.Add(-20 * time.Minute),
			MetricsSummary: map[string]any{
				"terminated_at": now.Add(-5 * time.Minute).Format(time.RFC3339),
				"diagnostics": map[string]any{
					"outcome":            "success",
					"failure_signatures": []any{},
				},
			},
		},
		{
			Namespace: "b",
			Status:    "failed",
			CreatedAt: now.Add(-10 * time.Minute),
			MetricsSummary: map[string]any{
				"terminated_at": now.Add(-1 * time.Minute).Format(time.RFC3339),
				"diagnostics": map[string]any{
					"outcome":            "failed",
					"failure_signatures": []any{"SCHEDULING_INSUFFICIENT_MEMORY"},
				},
			},
		},
	}

	report := buildReport(rows, 7, now)
	if report.TotalDeployments != 2 {
		t.Fatalf("total deploys: got %d", report.TotalDeployments)
	}
	if report.SignatureFrequency["SCHEDULING_INSUFFICIENT_MEMORY"].Count != 1 {
		t.Fatalf("expected signature frequency for scheduling failure, got %+v", report.SignatureFrequency)
	}
}
