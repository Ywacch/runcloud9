package main

import "testing"

func TestParseCacheTTLSeconds(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		def  int
		want int
	}{
		{name: "absent uses default", raw: "", def: 60, want: 60},
		{name: "zero means no expiry", raw: "0", def: 60, want: 0},
		{name: "timed expiry", raw: "90", def: 60, want: 90},
		{name: "negative uses default", raw: "-1", def: 60, want: 60},
		{name: "malformed uses default", raw: "abc", def: 60, want: 60},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseCacheTTLSeconds(tt.raw, tt.def); got != tt.want {
				t.Fatalf("parseCacheTTLSeconds(%q, %d) = %d, want %d", tt.raw, tt.def, got, tt.want)
			}
		})
	}
}

func TestCacheTTLSecondsFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
		generic string
		want    int
	}{
		{name: "neither set uses default", want: 60},
		{name: "generic CACHE_TTL applies", generic: "120", want: 120},
		{name: "catalog override shadows generic", catalog: "30", generic: "120", want: 30},
		{name: "zero generic means no expiry", generic: "0", want: 0},
		{name: "invalid override falls back to default not generic", catalog: "abc", generic: "120", want: 60},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.catalog != "" {
				t.Setenv("CATALOG_CACHE_TTL", tt.catalog)
			}
			if tt.generic != "" {
				t.Setenv("CACHE_TTL", tt.generic)
			}
			if got := cacheTTLSecondsFromEnv(60, "CATALOG_CACHE_TTL", "CACHE_TTL"); got != tt.want {
				t.Fatalf("cacheTTLSecondsFromEnv = %d, want %d", got, tt.want)
			}
		})
	}
}
