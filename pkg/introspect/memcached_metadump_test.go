package introspect

import (
	"testing"
	"time"
)

func TestParseMetadumpLine(t *testing.T) {
	t.Parallel()
	key, exp, size, ok := parseMetadumpLine("key=abc%3A1 exp=100 la=90 cas=1 fetch=yes cls=1 size=18")
	if !ok || key != "abc:1" || exp != 100 || size != 18 {
		t.Fatalf("parse = %q %d %d %v", key, exp, size, ok)
	}
	if _, _, _, ok := parseMetadumpLine("END"); ok {
		t.Fatal("END is not a key line")
	}
}

func TestMetadumpTTL(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000, 0)
	if got := metadumpTTL(0, now); got != -1 {
		t.Fatalf("no expiry: got %d", got)
	}
	if got := metadumpTTL(1_030, now); got != 30 {
		t.Fatalf("future: got %d", got)
	}
	if got := metadumpTTL(900, now); got != 0 {
		t.Fatalf("past: got %d", got)
	}
}
