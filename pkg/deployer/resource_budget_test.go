package deployer

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestTierResourceCaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tier    string
		wantCPU string
		wantMem string
	}{
		{tier: "explorer", wantCPU: "500m", wantMem: "1Gi"},
		{tier: "engineer", wantCPU: "1500m", wantMem: "3Gi"},
		{tier: "architect", wantCPU: "4", wantMem: "8Gi"},
		{tier: "", wantCPU: "500m", wantMem: "1Gi"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.tier, func(t *testing.T) {
			t.Parallel()
			cpu, mem := TierResourceCaps(tt.tier)
			assertQuantityEqual(t, cpu, tt.wantCPU, "cpu")
			assertQuantityEqual(t, mem, tt.wantMem, "memory")
		})
	}
}

func TestSumDesignResources_urlShortenerDefaults(t *testing.T) {
	t.Parallel()

	design := urlShortenerDefaultDesign()
	got := SumDesignResources(design, "explorer")
	assertQuantityEqual(t, got.CPU, "875m", "cpu total")
	assertQuantityEqual(t, got.Memory, "1408Mi", "memory total")
}

func TestSumDesignResources_urlShortenerLite(t *testing.T) {
	t.Parallel()

	got := SumDesignResources(urlShortenerLiteDesign(), "explorer")
	assertQuantityEqual(t, got.CPU, "625m", "cpu total")
	assertQuantityEqual(t, got.Memory, "1280Mi", "memory total")
}

func TestSumDesignResources_socialFeedPullLite(t *testing.T) {
	t.Parallel()

	got := SumDesignResources(socialFeedPullLiteDesign(), "explorer")
	assertQuantityEqual(t, got.CPU, "500m", "cpu total")
	assertQuantityEqual(t, got.Memory, "1Gi", "memory total")
}

func TestExplorerLiteBudget_maxAcrossTemplates(t *testing.T) {
	t.Parallel()

	designs := []struct {
		name   string
		design DeploymentDesign
	}{
		{name: "url-shortener", design: urlShortenerLiteDesign()},
		{name: "ecommerce", design: ecommerceLiteDesign()},
		{name: "social-feed", design: socialFeedLiteDesign()},
		{name: "social-feed-pull", design: socialFeedPullLiteDesign()},
	}

	var maxCPU, maxMem resource.Quantity
	for _, tc := range designs {
		got := SumDesignResources(tc.design, "explorer")
		if maxCPU.IsZero() || got.CPU.Cmp(maxCPU) > 0 {
			maxCPU = got.CPU.DeepCopy()
		}
		if maxMem.IsZero() || got.Memory.Cmp(maxMem) > 0 {
			maxMem = got.Memory.DeepCopy()
		}
	}
	assertQuantityEqual(t, maxCPU, "625m", "max lite cpu")
	assertQuantityEqual(t, maxMem, "1280Mi", "max lite memory")
}

func TestValidateResourceBudget_liteFailsExplorerToday(t *testing.T) {
	t.Parallel()

	err := ValidateResourceBudget(urlShortenerLiteDesign(), "explorer")
	var budgetErr *ResourceBudgetError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected ResourceBudgetError, got %v", err)
	}
	assertQuantityEqual(t, budgetErr.Requested.CPU, "625m", "requested cpu")
	assertQuantityEqual(t, budgetErr.Requested.Memory, "1280Mi", "requested memory")
}

func TestValidateResourceBudget_urlShortenerDefaultsFailsExplorer(t *testing.T) {
	t.Parallel()

	err := ValidateResourceBudget(urlShortenerDefaultDesign(), "explorer")
	var budgetErr *ResourceBudgetError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected ResourceBudgetError, got %v", err)
	}
	if budgetErr.Tier != "explorer" {
		t.Fatalf("tier: got %q want explorer", budgetErr.Tier)
	}
}

func TestValidateResourceBudget_scaledLargeFailsExplorer(t *testing.T) {
	t.Parallel()

	design := urlShortenerScaledLargeDesign()
	err := ValidateResourceBudget(design, "explorer")
	if err == nil {
		t.Fatal("expected error for scaled large design on explorer tier")
	}
}

func TestValidateResourceBudget_scaledLargePassesArchitect(t *testing.T) {
	t.Parallel()

	design := urlShortenerScaledLargeDesign()
	if err := ValidateResourceBudget(design, "architect"); err != nil {
		t.Fatalf("expected architect tier to pass: %v", err)
	}
}

func TestQuotaResourceLimits_proxyCacheDoublesMainWorkloads(t *testing.T) {
	t.Parallel()

	design := DeploymentDesign{
		Template: "url-shortener",
		Nodes: []DeploymentNode{
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]interface{}{
					"strategy": "write-through",
				},
			},
		},
	}
	NormalizeDesign(&design)

	user := SumDesignResources(design, "explorer")
	quota := QuotaResourceLimits(design, "explorer")

	assertQuantityEqual(t, user.CPU, "375m", "user cpu")
	assertQuantityEqual(t, SidecarOverhead(design).CPU, "25m", "exporter cpu")
	assertQuantityEqual(t, ProxyCacheOverhead(design).CPU, "125m", "proxy extra cpu")
	assertQuantityEqual(t, quota.CPU, "525m", "quota cpu with overhead")
}

func TestSidecarOverhead_cacheAsideOnlyOnRedis(t *testing.T) {
	t.Parallel()

	design := urlShortenerDefaultDesign()
	got := SidecarOverhead(design)
	assertQuantityEqual(t, got.CPU, "50m", "exporter cpu")
}

func assertQuantityEqual(t *testing.T, got resource.Quantity, want, label string) {
	t.Helper()
	wantQty := resource.MustParse(want)
	if got.Cmp(wantQty) != 0 {
		t.Fatalf("%s: got %v want %v", label, got, wantQty)
	}
}

func urlShortenerDefaultDesign() DeploymentDesign {
	d := DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "url-shortener",
		Nodes: []DeploymentNode{
			{ID: "lb-1", Type: "lb", Name: "Load Balancer"},
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "App Server",
				Config: map[string]interface{}{
					"replicas":     1,
					"instanceSize": "small",
				},
			},
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]interface{}{
					"strategy": "cache-aside",
				},
			},
			{ID: "db-1", Type: "db", Name: "PostgreSQL"},
		},
	}
	NormalizeDesign(&d)
	return d
}

func urlShortenerLiteDesign() DeploymentDesign {
	d := DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "url-shortener",
		Nodes: []DeploymentNode{
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "App Server",
				Config: map[string]interface{}{
					"replicas":     1,
					"instanceSize": "small",
				},
			},
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]interface{}{
					"strategy": "cache-aside",
				},
			},
			{ID: "db-1", Type: "db", Name: "PostgreSQL"},
		},
	}
	NormalizeDesign(&d)
	return d
}

func ecommerceLiteDesign() DeploymentDesign {
	d := urlShortenerLiteDesign()
	d.Template = "ecommerce"
	d.Archetype = "ecommerce"
	return d
}

func socialFeedLiteDesign() DeploymentDesign {
	d := urlShortenerLiteDesign()
	d.Template = "social-feed"
	d.Archetype = "social-feed"
	d.Nodes[1].Config["strategy"] = "timeline-store"
	d.Nodes[1].Config["cacheStrategy"] = "timeline-store"
	return d
}

func socialFeedPullLiteDesign() DeploymentDesign {
	d := DeploymentDesign{
		Template:  "social-feed-pull",
		Archetype: "social-feed-pull",
		Nodes: []DeploymentNode{
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "Feed Service",
				Config: map[string]interface{}{
					"replicas":     1,
					"instanceSize": "small",
				},
			},
			{ID: "db-1", Type: "db", Name: "PostgreSQL"},
		},
	}
	NormalizeDesign(&d)
	return d
}

func urlShortenerScaledLargeDesign() DeploymentDesign {
	d := DeploymentDesign{
		Template:  "url-shortener",
		Archetype: "url-shortener",
		Nodes: []DeploymentNode{
			{
				ID:   "lb-1",
				Type: "lb",
				Name: "Load Balancer",
				Config: map[string]interface{}{
					"instanceSize": "large",
				},
			},
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "App Server",
				Config: map[string]interface{}{
					"replicas":     3,
					"instanceSize": "large",
				},
			},
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]interface{}{
					"strategy":     "cache-aside",
					"instanceSize": "small",
				},
			},
			{
				ID:   "db-1",
				Type: "db",
				Name: "PostgreSQL",
				Config: map[string]interface{}{
					"instanceSize": "large",
				},
			},
		},
	}
	NormalizeDesign(&d)
	return d
}
