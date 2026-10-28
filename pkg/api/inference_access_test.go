package api

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

func TestInferenceAllowed(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)

	tests := []struct {
		name string
		user store.UserRecord
		want bool
	}{
		{name: "internal", user: store.UserRecord{IsInternal: true}, want: true},
		{
			name: "internal wins over expired premium",
			user: store.UserRecord{IsInternal: true, IsPremium: true, PremiumExpiresAt: &past},
			want: true,
		},
		{
			name: "engineer",
			user: store.UserRecord{IsPremium: true, PremiumExpiresAt: &future},
			want: false,
		},
		{
			name: "expired premium",
			user: store.UserRecord{IsPremium: true, PremiumExpiresAt: &past},
			want: false,
		},
		{name: "free", user: store.UserRecord{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := inferenceAllowed(tt.user); got != tt.want {
				t.Fatalf("inferenceAllowed() = %v, want %v", got, tt.want)
			}
		})
	}
}
