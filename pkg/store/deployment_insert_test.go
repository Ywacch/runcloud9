package store

import (
	"strings"
	"testing"
	"time"
)

func TestDeploymentInsertRow_SoloOmitsComparisonColumns(t *testing.T) {
	t.Parallel()
	exp := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	row, err := DeploymentInsertRow(CreateDeploymentParams{
		Namespace: "ns-solo",
		Template:  "social-feed",
		ExpiresAt: exp,
	}, "provisioning")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := row["comparison_id"]; ok {
		t.Fatal("solo insert must omit comparison_id")
	}
	if _, ok := row["run_role"]; ok {
		t.Fatal("solo insert must omit run_role")
	}
}

func TestDeploymentInsertRow_WithRole(t *testing.T) {
	t.Parallel()
	exp := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	cmp := "550e8400-e29b-41d4-a716-446655440000"
	row, err := DeploymentInsertRow(CreateDeploymentParams{
		Namespace:    "ns-cand",
		Template:     "social-feed",
		ExpiresAt:    exp,
		ComparisonID: cmp,
		RunRole:      RunRoleCandidate,
	}, "provisioning")
	if err != nil {
		t.Fatal(err)
	}
	if row["comparison_id"] != cmp {
		t.Fatalf("comparison_id: %v", row["comparison_id"])
	}
	if row["run_role"] != RunRoleCandidate {
		t.Fatalf("run_role: %v", row["run_role"])
	}
}

func TestValidateComparisonFields(t *testing.T) {
	t.Parallel()
	cmp := "550e8400-e29b-41d4-a716-446655440000"
	cases := []struct {
		name    string
		cmp     string
		role    string
		wantErr string
	}{
		{name: "ok solo", cmp: "", role: ""},
		{name: "ok pair", cmp: cmp, role: RunRoleBaseline},
		{name: "bad role", cmp: cmp, role: "solo", wantErr: "invalid run role"},
		{name: "role without id", cmp: "", role: RunRoleCandidate, wantErr: "requires comparison_id"},
		{name: "id without role", cmp: cmp, role: "", wantErr: "requires run_role"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateComparisonFields(tc.cmp, tc.role)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}
