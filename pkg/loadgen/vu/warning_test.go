package vu

import (
	"strings"
	"testing"
)

func TestReplyGapWarning(t *testing.T) {
	tests := []struct {
		name   string
		delays []int
		within int
		want   string
	}{
		{"staggered ok", []int{0, 1500}, 300, ""},
		{"equal delay", []int{1500, 1500}, 300, "reply gap too wide"},
		{"narrow stagger", []int{0, 400}, 300, "reply gap too wide"},
		{"empty delays", nil, 300, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ReplyGapWarning(tc.within, tc.delays)
			if tc.want == "" {
				if got != "" {
					t.Fatalf("want empty, got %q", got)
				}
				return
			}
			if !strings.Contains(got, tc.want) {
				t.Fatalf("got %q", got)
			}
		})
	}
}
