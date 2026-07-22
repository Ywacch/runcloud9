// Package admission implements the cluster room budget gate for Cloud9 deploys.
//
// Room size comes from CLUSTER_DEPLOY_BUDGET_CPU / CLUSTER_DEPLOY_BUDGET_MEMORY.
// When both are unset the gate is off. Person size is the design's
// QuotaResourceLimits envelope (call WeightFromBudget at the API boundary).
//
// Assumes a single cloud9-api replica: multi-replica admission needs DB-atomic
// checks before horizontal scale.
package admission

import (
	"log"
	"os"
	"strings"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Weight is one deployment's CPU/memory envelope in millicores and mebibytes.
type Weight struct {
	CPUMillicores int64
	MemoryMi      int64
}

// Occupancy is the sum of admitted weights for active deployments.
type Occupancy struct {
	CPUMillicores int64
	MemoryMi      int64
}

// Config is the cluster room budget. A nil *Config means the gate is disabled.
type Config struct {
	CPUMillicores int64
	MemoryMi      int64
}

// ConfigFromEnv reads CLUSTER_DEPLOY_BUDGET_CPU and CLUSTER_DEPLOY_BUDGET_MEMORY.
// Both unset → nil (gate off). Exactly one set or invalid parse → nil and a log line.
func ConfigFromEnv() *Config {
	cpuRaw := strings.TrimSpace(os.Getenv("CLUSTER_DEPLOY_BUDGET_CPU"))
	memRaw := strings.TrimSpace(os.Getenv("CLUSTER_DEPLOY_BUDGET_MEMORY"))
	if cpuRaw == "" && memRaw == "" {
		return nil
	}
	if cpuRaw == "" || memRaw == "" {
		log.Printf("admission: set both CLUSTER_DEPLOY_BUDGET_CPU and CLUSTER_DEPLOY_BUDGET_MEMORY (or neither); disabling room gate")
		return nil
	}
	cpuQ, err := resource.ParseQuantity(cpuRaw)
	if err != nil {
		log.Printf("admission: invalid CLUSTER_DEPLOY_BUDGET_CPU %q: %v (disabling room gate)", cpuRaw, err)
		return nil
	}
	memQ, err := resource.ParseQuantity(memRaw)
	if err != nil {
		log.Printf("admission: invalid CLUSTER_DEPLOY_BUDGET_MEMORY %q: %v (disabling room gate)", memRaw, err)
		return nil
	}
	cpuM := cpuQ.MilliValue()
	memMi := memQ.Value() / (1024 * 1024)
	if cpuM <= 0 || memMi <= 0 {
		log.Printf("admission: budget must be positive (cpu_m=%d mem_mi=%d); disabling room gate", cpuM, memMi)
		return nil
	}
	return &Config{CPUMillicores: cpuM, MemoryMi: memMi}
}

// Fits reports whether occupancy plus weight fits inside the room budget.
// Caller must not pass a nil Config.
func Fits(cfg *Config, occ Occupancy, w Weight) bool {
	if cfg == nil {
		return true
	}
	return occ.CPUMillicores+w.CPUMillicores <= cfg.CPUMillicores &&
		occ.MemoryMi+w.MemoryMi <= cfg.MemoryMi
}

// ExceedsRoom reports whether weight alone cannot fit an empty room.
// A nil Config means the gate is off → never exceeds.
func ExceedsRoom(cfg *Config, w Weight) bool {
	if cfg == nil {
		return false
	}
	return !Fits(cfg, Occupancy{}, w)
}

// WeightFromBudget converts a QuotaResourceLimits (or similar) budget into Weight.
func WeightFromBudget(b deployer.ResourceBudget) Weight {
	return Weight{
		CPUMillicores: b.CPU.MilliValue(),
		MemoryMi:      b.Memory.Value() / (1024 * 1024),
	}
}
