package main

import "testing"

func TestUserPoolSQL_segmentQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		segment string
		limit   int
		want    string
	}{
		{"", 50, `SELECT id FROM users ORDER BY id LIMIT 50`},
		{"celebrity", 500, `SELECT id FROM users WHERE id <= 10 ORDER BY id`},
		{"normal", 500, `SELECT id FROM users WHERE id > 10 ORDER BY id LIMIT 500`},
		{"unknown", 50, `SELECT id FROM users ORDER BY id LIMIT 50`},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.segment, func(t *testing.T) {
			t.Parallel()
			if got := userPoolSQL(tc.segment, tc.limit); got != tc.want {
				t.Fatalf("userPoolSQL(%q, %d): got %q want %q", tc.segment, tc.limit, got, tc.want)
			}
		})
	}
}

func TestParseUserPoolLimit(t *testing.T) {
	t.Parallel()
	if parseUserPoolLimit("") != 50 {
		t.Fatal("empty")
	}
	if parseUserPoolLimit("500") != 500 {
		t.Fatal("500")
	}
	if parseUserPoolLimit("0") != 50 || parseUserPoolLimit("10001") != 50 {
		t.Fatal("bad")
	}
}
