package main

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

func TestVerboseLogsEnabled(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	if trace.VerboseEnabled() {
		t.Fatal("expected false when unset")
	}
	t.Setenv("LOG_VERBOSE", "1")
	if !trace.VerboseEnabled() {
		t.Fatal("expected true when LOG_VERBOSE=1")
	}
	t.Setenv("LOG_VERBOSE", "0")
	if trace.VerboseEnabled() {
		t.Fatal("expected false when LOG_VERBOSE=0")
	}
}
