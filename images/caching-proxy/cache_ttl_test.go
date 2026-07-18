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
		{name: "timed expiry", raw: "45", def: 300, want: 45},
		{name: "negative uses default", raw: "-1", def: 300, want: 300},
		{name: "malformed uses default", raw: "abc", def: 300, want: 300},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseCacheTTLSeconds(tt.raw, tt.def); got != tt.want {
				t.Fatalf("parseCacheTTLSeconds(%q, %d) = %d, want %d", tt.raw, tt.def, got, tt.want)
			}
		})
	}
}
