package admission

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestConfigFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		cpu     string
		mem     string
		wantNil bool
		wantCPU int64
		wantMem int64
	}{
		{name: "both unset", wantNil: true},
		{name: "cpu only", cpu: "18500m", wantNil: true},
		{name: "mem only", mem: "49920Mi", wantNil: true},
		{name: "both set", cpu: "18500m", mem: "49920Mi", wantCPU: 18500, wantMem: 49920},
		{name: "bad cpu", cpu: "not-a-quantity", mem: "49920Mi", wantNil: true},
		{name: "zero cpu", cpu: "0", mem: "49920Mi", wantNil: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", tt.cpu)
			t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", tt.mem)
			cfg := ConfigFromEnv()
			if tt.wantNil {
				if cfg != nil {
					t.Fatalf("ConfigFromEnv() = %+v, want nil", cfg)
				}
				return
			}
			if cfg == nil {
				t.Fatal("ConfigFromEnv() = nil, want config")
			}
			if cfg.CPUMillicores != tt.wantCPU || cfg.MemoryMi != tt.wantMem {
				t.Fatalf("ConfigFromEnv() = %+v, want cpu=%d mem=%d", cfg, tt.wantCPU, tt.wantMem)
			}
		})
	}
}

func TestFits(t *testing.T) {
	t.Parallel()

	cfg := &Config{CPUMillicores: 1000, MemoryMi: 2000}
	tests := []struct {
		name string
		cfg  *Config
		occ  Occupancy
		w    Weight
		want bool
	}{
		{name: "under", cfg: cfg, occ: Occupancy{100, 200}, w: Weight{100, 200}, want: true},
		{name: "exact", cfg: cfg, occ: Occupancy{500, 1000}, w: Weight{500, 1000}, want: true},
		{name: "over cpu", cfg: cfg, occ: Occupancy{900, 0}, w: Weight{200, 0}, want: false},
		{name: "over mem", cfg: cfg, occ: Occupancy{0, 1900}, w: Weight{0, 200}, want: false},
		{name: "nil cfg admits", cfg: nil, occ: Occupancy{9e6, 9e6}, w: Weight{9e6, 9e6}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Fits(tt.cfg, tt.occ, tt.w); got != tt.want {
				t.Fatalf("Fits() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWeightFromBudget(t *testing.T) {
	t.Parallel()
	b := deployer.ResourceBudget{
		CPU:    resource.MustParse("925m"),
		Memory: resource.MustParse("2496Mi"),
	}
	w := WeightFromBudget(b)
	if w.CPUMillicores != 925 || w.MemoryMi != 2496 {
		t.Fatalf("WeightFromBudget() = %+v, want cpu=925 mem=2496", w)
	}
}
