package main

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	t.Parallel()
	values := []int64{10, 20, 30, 40, 50}
	if got := percentile(values, 50); got != 30 {
		t.Fatalf("p50=%d", got)
	}
	if got := percentile(values, 95); got != 50 {
		t.Fatalf("p95=%d", got)
	}
	if got := percentile(nil, 50); got != 0 {
		t.Fatalf("empty=%d", got)
	}
}

func TestPeakSimultaneous(t *testing.T) {
	t.Parallel()
	base := time.Unix(0, 0).UTC()
	intervals := []timeInterval{
		{start: base, end: base.Add(10 * time.Second)},
		{start: base.Add(5 * time.Second), end: base.Add(15 * time.Second)},
		{start: base.Add(12 * time.Second), end: base.Add(20 * time.Second)},
	}
	if got := peakSimultaneous(intervals); got != 2 {
		t.Fatalf("peak=%d want 2", got)
	}
}

func TestSummarizeWave(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	results := []DeploymentResult{
		{
			Index: 0, Design: "url-shortener-small", Weight: DeploymentWeight{CPUMillicores: 100, MemoryMi: 200},
			HTTPStatus: 201, DeploymentID: "a", CreateStatus: "provisioning", TerminalStatus: "running",
			CreatedAt: now, TerminalAt: now.Add(2 * time.Second), TimeToRunningMS: 2000,
		},
		{
			Index: 1, Design: "url-shortener-small", Weight: DeploymentWeight{CPUMillicores: 100, MemoryMi: 200},
			HTTPStatus: 429, CreateStatus: "rejected", FailureReason: "active deployment",
			CreatedAt: now,
		},
		{
			Index: 2, Design: "url-shortener-small", Weight: DeploymentWeight{CPUMillicores: 100, MemoryMi: 200},
			HTTPStatus: 201, DeploymentID: "c", CreateStatus: "provisioning", TerminalStatus: "failed",
			FailureReason: "timeout", CreatedAt: now, TerminalAt: now.Add(time.Second),
		},
	}
	rep := summarizeWave("w", "burst", results, nil)
	if rep.RequestedCount != 3 || rep.AdmittedCount != 2 || rep.SuccessCount != 1 || rep.FailedCount != 1 || rep.RejectedCount != 1 {
		t.Fatalf("counts: %+v", rep)
	}
	if rep.SuccessfulWeight.CPUMillicores != 100 {
		t.Fatalf("successful weight: %+v", rep.SuccessfulWeight)
	}
	if rep.FailureBreakdown["active deployment"] != 1 || rep.FailureBreakdown["timeout"] != 1 {
		t.Fatalf("breakdown: %+v", rep.FailureBreakdown)
	}
}
