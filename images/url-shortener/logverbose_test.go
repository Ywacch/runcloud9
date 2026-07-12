package main

import "testing"

func TestVerboseLogsEnabled(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	if verboseLogsEnabled() {
		t.Fatal("expected false when unset")
	}
	t.Setenv("LOG_VERBOSE", "1")
	if !verboseLogsEnabled() {
		t.Fatal("expected true when LOG_VERBOSE=1")
	}
	t.Setenv("LOG_VERBOSE", "0")
	if verboseLogsEnabled() {
		t.Fatal("expected false when LOG_VERBOSE=0")
	}
}
