package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// waitForMeasureHold stalls DELETE until releasePath exists so a scraper can
// read sessions that are still running. An empty releasePath returns
// immediately and does not write readyPath.
func waitForMeasureHold(ctx context.Context, readyPath, releasePath string, timeout, poll time.Duration) error {
	releasePath = strings.TrimSpace(releasePath)
	if releasePath == "" {
		return nil
	}
	readyPath = strings.TrimSpace(readyPath)
	if readyPath != "" {
		if err := writeReadyFile(readyPath); err != nil {
			return err
		}
	}
	if releaseFileExists(releasePath) {
		return nil
	}
	if timeout < 0 {
		return fmt.Errorf("capacity-probe: cleanup hold timeout must be >= 0")
	}
	if poll <= 0 {
		poll = time.Second
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("capacity-probe: cleanup hold canceled: %w", ctx.Err())
		case <-ticker.C:
			if releaseFileExists(releasePath) {
				return nil
			}
			if !time.Now().Before(deadline) {
				return fmt.Errorf("capacity-probe: cleanup hold timed out waiting for %s", releasePath)
			}
		}
	}
}

func writeReadyFile(path string) error {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("capacity-probe: cleanup ready dir: %w", err)
		}
	}
	body := []byte(time.Now().UTC().Format(time.RFC3339Nano) + "\n")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("capacity-probe: cleanup ready file: %w", err)
	}
	return nil
}

func releaseFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
