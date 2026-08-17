package main

import "testing"

func TestUserPoolSQL_segmentQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		segment string
		want    string
	}{
		{"", `SELECT id FROM users ORDER BY id LIMIT 50`},
		{"celebrity", `SELECT id FROM users WHERE id <= 10 ORDER BY id`},
		{"normal", `SELECT id FROM users WHERE id > 10 ORDER BY id LIMIT 50`},
		{"unknown", `SELECT id FROM users ORDER BY id LIMIT 50`},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.segment, func(t *testing.T) {
			t.Parallel()
			if got := userPoolSQL(tc.segment); got != tc.want {
				t.Fatalf("userPoolSQL(%q): got %q want %q", tc.segment, got, tc.want)
			}
		})
	}
}
