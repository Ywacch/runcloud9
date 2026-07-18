package main

import "testing"

func TestParseCacheTTLSeconds(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		def  int
		want int
	}{
		{name: "absent uses default", raw: "", def: 300, want: 300},
		{name: "zero means no expiry", raw: "0", def: 300, want: 0},
		{name: "timed expiry", raw: "120", def: 300, want: 120},
		{name: "negative uses default", raw: "-5", def: 300, want: 300},
		{name: "malformed uses default", raw: "soon", def: 300, want: 300},
		{name: "whitespace trimmed", raw: " 60 ", def: 300, want: 60},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseCacheTTLSeconds(tt.raw, tt.def); got != tt.want {
				t.Fatalf("parseCacheTTLSeconds(%q, %d) = %d, want %d", tt.raw, tt.def, got, tt.want)
			}
		})
	}
}
