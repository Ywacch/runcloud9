package store

import (
	"fmt"
	"strings"
	"time"
)

const (
	RunRoleBaseline  = "baseline"
	RunRoleCandidate = "candidate"
)

// ValidateComparisonFields ensures comparison id and run role are paired and valid.
func ValidateComparisonFields(comparisonID, runRole string) error {
	cmp := strings.TrimSpace(comparisonID)
	role := strings.TrimSpace(runRole)
	if cmp == "" && role == "" {
		return nil
	}
	if cmp == "" {
		return fmt.Errorf("store.CreateDeployment: run_role %q requires comparison_id", role)
	}
	if role == "" {
		return fmt.Errorf("store.CreateDeployment: comparison_id requires run_role")
	}
	if role != RunRoleBaseline && role != RunRoleCandidate {
		return fmt.Errorf("store.CreateDeployment: invalid run role %q", role)
	}
	return nil
}

// DeploymentInsertRow builds the PostgREST insert map for cloud_deployments.
// Solo runs omit comparison_id and run_role so inserts match pre-S-1 rows.
func DeploymentInsertRow(params CreateDeploymentParams, status string) (map[string]any, error) {
	if err := ValidateComparisonFields(params.ComparisonID, params.RunRole); err != nil {
		return nil, err
	}
	userIDVal := any(params.UserID)
	if params.UserID == "" {
		userIDVal = nil
	}
	row := map[string]any{
		"namespace":       params.Namespace,
		"template":        params.Template,
		"archetype":       params.Archetype,
		"status":          status,
		"design":          params.Design,
		"expires_at":      params.ExpiresAt.Format(time.RFC3339),
		"user_id":         userIDVal,
		"admitted_cpu_m":  params.AdmittedCPUm,
		"admitted_mem_mi": params.AdmittedMemMi,
	}
	if strings.TrimSpace(params.ComparisonID) != "" {
		row["comparison_id"] = strings.TrimSpace(params.ComparisonID)
		row["run_role"] = strings.TrimSpace(params.RunRole)
	}
	return row, nil
}
