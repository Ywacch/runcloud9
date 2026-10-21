package deployer

import (
	"strings"
	"testing"
)

func TestParseReplicaApplyDelayMs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		config       map[string]any
		readReplicas int
		want         []int
		wantErr      string
	}{
		{
			name:         "missing defaults zero",
			config:       map[string]any{"readReplicas": 2},
			readReplicas: 2,
			want:         []int{0, 0},
		},
		{
			name:         "array per standby",
			config:       map[string]any{"replicaApplyDelayMs": []any{0, 1500}},
			readReplicas: 2,
			want:         []int{0, 1500},
		},
		{
			name:         "scalar expands",
			config:       map[string]any{"replicaApplyDelayMs": 1500},
			readReplicas: 2,
			want:         []int{1500, 1500},
		},
		{
			name:         "comma string",
			config:       map[string]any{"replicaApplyDelayMs": "0,1500"},
			readReplicas: 2,
			want:         []int{0, 1500},
		},
		{
			name:         "length mismatch",
			config:       map[string]any{"replicaApplyDelayMs": []any{0}},
			readReplicas: 2,
			wantErr:      "length must equal readReplicas",
		},
		{
			name:         "out of range high",
			config:       map[string]any{"replicaApplyDelayMs": 5001},
			readReplicas: 1,
			wantErr:      "must be between 0 and 5000",
		},
		{
			name:         "out of range negative",
			config:       map[string]any{"replicaApplyDelayMs": -1},
			readReplicas: 1,
			wantErr:      "must be between 0 and 5000",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseReplicaApplyDelayMs(tc.config, tc.readReplicas)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("delays: got %v want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("delays[%d]: got %d want %d", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestBuildDBReplicaInitScript_applyDelayCase(t *testing.T) {
	t.Parallel()

	cfg := DeployConfig{
		Node: DeploymentNode{
			Config: map[string]any{
				"readReplicas":        2,
				"replicaApplyDelayMs": []any{0, 1500},
			},
		},
	}
	script := buildDBReplicaInitScript(cfg)
	for _, frag := range []string{
		"postgres-replica-0",
		"recovery_min_apply_delay = '0ms'",
		"postgres-replica-1",
		"recovery_min_apply_delay = '1500ms'",
		`case "$HOSTNAME" in`,
	} {
		if !strings.Contains(script, frag) {
			t.Fatalf("script missing %q:\n%s", frag, script)
		}
	}
}

func TestReplicaApplyDelayUniform(t *testing.T) {
	t.Parallel()
	if !ReplicaApplyDelayUniform([]int{100, 100}) {
		t.Fatal("expected uniform")
	}
	if ReplicaApplyDelayUniform([]int{0, 1500}) {
		t.Fatal("expected non-uniform")
	}
}
