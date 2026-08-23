package main

import (
	"testing"
)

func TestProductStockEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		served string
		truth  string
		want   bool
	}{
		{
			name:   "same stock",
			served: `{"id":1,"name":"Widget","price":9.99,"stock":42}`,
			truth:  "42",
			want:   true,
		},
		{
			name:   "different stock",
			served: `{"id":1,"name":"Widget","price":9.99,"stock":3}`,
			truth:  "0",
			want:   false,
		},
		{
			name:   "truth whitespace",
			served: `{"id":2,"stock":7,"name":"X","price":1}`,
			truth:  " 7 ",
			want:   true,
		},
		{
			name:   "bad json skips stale",
			served: `not-json`,
			truth:  "1",
			want:   true,
		},
		{
			name:   "empty served skips stale",
			served: ``,
			truth:  "5",
			want:   true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := productStockEqual(tc.served, tc.truth); got != tc.want {
				t.Fatalf("productStockEqual() = %v, want %v", got, tc.want)
			}
		})
	}
}
