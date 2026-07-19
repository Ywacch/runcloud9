package main

import (
	"github.com/ywacch/cloud9/pkg/deployer"
)

// DeploymentWeight is the cluster resource envelope for one deployment.
type DeploymentWeight struct {
	CPUMillicores int64 `json:"cpu_millicores"`
	MemoryMi      int64 `json:"memory_mi"`
}

func computeWeight(design deployer.DeploymentDesign, tier string) DeploymentWeight {
	budget := deployer.QuotaResourceLimits(design, tier)
	return DeploymentWeight{
		CPUMillicores: budget.CPU.MilliValue(),
		MemoryMi:      budget.Memory.Value() / (1024 * 1024),
	}
}

func scaleWeight(w DeploymentWeight, n int) DeploymentWeight {
	if n < 1 {
		return DeploymentWeight{}
	}
	return DeploymentWeight{
		CPUMillicores: w.CPUMillicores * int64(n),
		MemoryMi:      w.MemoryMi * int64(n),
	}
}

func addWeight(a, b DeploymentWeight) DeploymentWeight {
	return DeploymentWeight{
		CPUMillicores: a.CPUMillicores + b.CPUMillicores,
		MemoryMi:      a.MemoryMi + b.MemoryMi,
	}
}
