package api

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

func TestHasRecentActiveCreate(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	window := 3 * time.Second

	tests := []struct {
		name   string
		active []store.ActiveDeploymentRow
		want   bool
	}{
		{
			name:   "empty active",
			active: nil,
			want:   false,
		},
		{
			name: "CreatedAt zero",
			active: []store.ActiveDeploymentRow{{
				ID:        "dep-1",
				Namespace: "ns-1",
				Status:    "provisioning",
			}},
			want: false,
		},
		{
			name: "CreatedAt 146ms ago",
			active: []store.ActiveDeploymentRow{{
				ID:        "dep-1",
				Namespace: "ns-1",
				Status:    "provisioning",
				CreatedAt: now.Add(-146 * time.Millisecond),
			}},
			want: true,
		},
		{
			name: "CreatedAt 4s ago",
			active: []store.ActiveDeploymentRow{{
				ID:        "dep-1",
				Namespace: "ns-1",
				Status:    "provisioning",
				CreatedAt: now.Add(-4 * time.Second),
			}},
			want: false,
		},
		{
			name: "mixed rows one recent",
			active: []store.ActiveDeploymentRow{
				{
					ID:        "dep-old",
					Namespace: "ns-old",
					Status:    "running",
					CreatedAt: now.Add(-2 * time.Minute),
				},
				{
					ID:        "dep-recent",
					Namespace: "ns-recent",
					Status:    "provisioning",
					CreatedAt: now.Add(-500 * time.Millisecond),
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasRecentActiveCreate(tt.active, now, window)
			if got != tt.want {
				t.Fatalf("hasRecentActiveCreate() = %v, want %v", got, tt.want)
			}
		})
	}
}
