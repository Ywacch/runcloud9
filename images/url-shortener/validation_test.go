package main

import (
	"strings"
	"testing"
)

func TestValidateShortenURL(t *testing.T) {
	t.Parallel()
	longOK := "https://x/" + strings.Repeat("a", 2038)
	if len(longOK) != 2048 {
		t.Fatalf("test setup: len=%d want 2048", len(longOK))
	}
	longBad := "https://x/" + strings.Repeat("a", 2039)

	for _, tc := range []struct {
		raw string
		ok  bool
	}{
		{"", false},
		{"https://example.com/p?q=1", true},
		{"HTTP://EXAMPLE.COM/", true},
		{"http://localhost:8080/", true},
		{"ftp://example.com/x", false},
		{"not-a-uri", false},
		{"https://", false},
		{longOK, true},
		{longBad, false},
	} {
		err := validateShortenURL(tc.raw)
		if tc.ok && err != nil {
			t.Errorf("%q: want ok, got %v", tc.raw, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("%q: want err, got nil", tc.raw)
		}
	}
}
