package deployer

import (
	"testing"
)

func TestNormalizeInstanceSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw  string
		want string
	}{
		{"small", "small"},
		{"medium", "medium"},
		{"large", "large"},
		{"alpha", "small"},
		{"ALPHA", "small"},
		{"", "small"},
		{"huge", "small"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.raw, func(t *testing.T) {
			t.Parallel()
			if got := normalizeInstanceSize(tc.raw); got != tc.want {
				t.Fatalf("normalizeInstanceSize(%q) = %q want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestInstanceSizesFromConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		config   map[string]any
		replicas int
		want     []string
	}{
		{
			name:     "defaults from instanceSize",
			config:   map[string]any{"instanceSize": "large"},
			replicas: 2,
			want:     []string{"large", "large"},
		},
		{
			name: "array any pads short",
			config: map[string]any{
				"instanceSize":  "small",
				"instanceSizes": []any{"large"},
			},
			replicas: 3,
			want:     []string{"large", "small", "small"},
		},
		{
			name: "array string truncates extra",
			config: map[string]any{
				"instanceSize":  "small",
				"instanceSizes": []string{"medium", "large", "small"},
			},
			replicas: 2,
			want:     []string{"medium", "large"},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := instanceSizesFromConfig(tc.config, tc.replicas)
			if len(got) != len(tc.want) {
				t.Fatalf("len = %d want %d: %v", len(got), len(tc.want), got)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("[%d] = %q want %q (full %v)", i, got[i], tc.want[i], got)
				}
			}
		})
	}
}

func TestSizeGroups(t *testing.T) {
	t.Parallel()
	groups := sizeGroups([]string{"large", "large", "small"})
	if len(groups) != 2 {
		t.Fatalf("len = %d want 2", len(groups))
	}
	if groups[0].Size != "large" || groups[0].Count != 2 {
		t.Fatalf("first group: %+v", groups[0])
	}
	if groups[1].Size != "small" || groups[1].Count != 1 {
		t.Fatalf("second group: %+v", groups[1])
	}
}

func TestNginxWeightForSize(t *testing.T) {
	t.Parallel()
	if nginxWeightForSize("small") != 1 {
		t.Fatal("small")
	}
	if nginxWeightForSize("medium") != 2 {
		t.Fatal("medium")
	}
	if nginxWeightForSize("large") != 4 {
		t.Fatal("large")
	}
}
