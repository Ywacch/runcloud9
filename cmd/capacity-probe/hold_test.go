package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWaitForMeasureHold_EmptyReleaseSkipsReadyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ready := filepath.Join(dir, "wave-ready")
	err := waitForMeasureHold(context.Background(), ready, "", time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(ready); !os.IsNotExist(statErr) {
		t.Fatalf("ready file exists or stat failed: %v", statErr)
	}
}

func TestWaitForMeasureHold_ReleaseAlreadyPresent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ready := filepath.Join(dir, "nested", "wave-ready")
	release := filepath.Join(dir, "measure-done")
	if err := os.WriteFile(release, []byte("done\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := waitForMeasureHold(context.Background(), ready, release, 0, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(ready); statErr != nil {
		t.Fatal(statErr)
	}
}

func TestWaitForMeasureHold_FileAppears(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ready := filepath.Join(dir, "wave-ready")
	release := filepath.Join(dir, "measure-done")
	go func() {
		time.Sleep(30 * time.Millisecond)
		// A failed write leaves the release file missing and the wait times out.
		_ = os.WriteFile(release, []byte("done\n"), 0o644)
	}()
	err := waitForMeasureHold(context.Background(), ready, release, 2*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWaitForMeasureHold_Timeout(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	release := filepath.Join(dir, "measure-done")
	err := waitForMeasureHold(context.Background(), "", release, 40*time.Millisecond, 10*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v, want timeout", err)
	}
}

func TestWaitForMeasureHold_Canceled(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	release := filepath.Join(dir, "measure-done")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitForMeasureHold(ctx, "", release, time.Minute, 10*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
