package deployer

import (
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
		{tier: "explorer", wantCPU: "1075m", wantMem: "2048Mi"},
		{tier: "engineer", wantCPU: "3375m", wantMem: "6912Mi"},
		{tier: "internal", wantCPU: "4250m", wantMem: "8320Mi"},
		{tier: "architect", wantCPU: "4", wantMem: "8Gi"},
		{tier: "", wantCPU: "1075m", wantMem: "2048Mi"},
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

func TestSumDesignResources_dbReadReplicasIncreaseBudget(t *testing.T) {
	t.Parallel()

	base := DeploymentDesign{
		Nodes: []DeploymentNode{{
			ID:     "db-1",
			Type:   "db",
			Name:   "PostgreSQL",
			Config: map[string]interface{}{"instanceSize": "small"},
		}},
	}
	withReplica := DeploymentDesign{
		Nodes: []DeploymentNode{{
			ID:   "db-1",
			Type: "db",
			Name: "PostgreSQL",
			Config: map[string]interface{}{
				"instanceSize": "small",
				"readReplicas": 1,
			},
		}},
	}
	gotBase := SumDesignResources(base, "explorer")
	gotReplica := SumDesignResources(withReplica, "explorer")
	if gotReplica.CPU.Cmp(gotBase.CPU) <= 0 {
		t.Fatalf("db with read replica CPU %v should exceed base %v", gotReplica.CPU, gotBase.CPU)
	}
	if gotReplica.Memory.Cmp(gotBase.Memory) <= 0 {
		t.Fatalf("db with read replica memory %v should exceed base %v", gotReplica.Memory, gotBase.Memory)
	}
}

func TestSumDesignResources_mixedInstanceSizes(t *testing.T) {
	t.Parallel()

	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{
				ID:   "app1",
				Type: "app-server",
				Name: "App Server",
				Config: map[string]interface{}{
					"replicas":      2,
					"instanceSize":  "large",
					"instanceSizes": []any{"large", "small"},
				},
			},
		},
	}
	got := SumDesignResources(design, "explorer")
	twoLarge := DeploymentDesign{
		Nodes: []DeploymentNode{
			{
				ID:   "app1",
				Type: "app-server",
				Name: "App Server",
				Config: map[string]interface{}{
					"replicas":     2,
					"instanceSize": "large",
				},
			},
		},
	}
	gotTwoLarge := SumDesignResources(twoLarge, "explorer")
	if got.CPU.Cmp(gotTwoLarge.CPU) >= 0 {
		t.Fatalf("mixed large+small CPU %v should be less than 2× large %v", got.CPU, gotTwoLarge.CPU)
	}
}

func TestSumDesignResources_dbMixedInstanceSizes(t *testing.T) {
	t.Parallel()

	design := DeploymentDesign{
		Nodes: []DeploymentNode{{
			ID:   "db-1",
			Type: "db",
			Name: "PostgreSQL",
			Config: map[string]interface{}{
				"readReplicas":  1,
				"instanceSize":  "large",
				"instanceSizes": []any{"large", "small"},
			},
		}},
	}
	got := SumDesignResources(design, "explorer")
	twoLarge := DeploymentDesign{
		Nodes: []DeploymentNode{{
			ID:   "db-1",
			Type: "db",
			Name: "PostgreSQL",
			Config: map[string]interface{}{
				"readReplicas": 1,
				"instanceSize": "large",
			},
		}},
	}
	gotTwoLarge := SumDesignResources(twoLarge, "explorer")
	if got.CPU.Cmp(gotTwoLarge.CPU) >= 0 {
		t.Fatalf("db large primary + small replica CPU %v should be less than 2× large %v", got.CPU, gotTwoLarge.CPU)
	}
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

func TestValidateResourceBudget_litePassesExplorer(t *testing.T) {
	t.Parallel()

	if err := ValidateResourceBudget(urlShortenerLiteDesign(), "explorer"); err != nil {
		t.Fatalf("expected lite design to fit explorer cap: %v", err)
	}
}

func TestValidateResourceBudget_urlShortenerDefaultsPassesExplorer(t *testing.T) {
	t.Parallel()

	err := ValidateResourceBudget(urlShortenerDefaultDesign(), "explorer")
	if err != nil {
		t.Fatalf("expected default design to fit explorer cap: %v", err)
	}
}

func TestValidateResourceBudget_socialFeedAsync(t *testing.T) {
	t.Parallel()

	design := socialFeedAsyncDesign()
	got := SumDesignResources(design, "explorer")
	assertQuantityEqual(t, got.CPU, "950m", "async all-small cpu")
	if err := ValidateResourceBudget(design, "explorer"); err != nil {
		t.Fatalf("expected async all-small to fit explorer cap: %v", err)
	}

	oneMedium := socialFeedAsyncDesign()
	oneMedium.Nodes[0].Config["instanceSize"] = "medium"
	if err := ValidateResourceBudget(oneMedium, "explorer"); err != nil {
		t.Fatalf("expected async + one medium to fit explorer cap: %v", err)
	}

	twoMedium := socialFeedAsyncDesign()
	twoMedium.Nodes[0].Config["instanceSize"] = "medium"
	twoMedium.Nodes[2].Config["instanceSize"] = "medium"
	if err := ValidateResourceBudget(twoMedium, "explorer"); err == nil {
		t.Fatal("expected async + two mediums to exceed explorer cap")
	}
}

func TestValidateResourceBudget_urlShortenerAppDBExtraReplicaPassesExplorer(t *testing.T) {
	t.Parallel()

	design := urlShortenerAppDBDesign()
	design.Nodes[0].Config["replicas"] = 2
	if err := ValidateResourceBudget(design, "explorer"); err != nil {
		t.Fatalf("expected skinny design + extra replica to fit: %v", err)
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

func TestValidateResourceBudget_urlShortenerDefaultsPassesEngineerAndInternal(t *testing.T) {
	t.Parallel()

	design := urlShortenerDefaultDesign()
	for _, tier := range []string{"engineer", "internal"} {
		tier := tier
		t.Run(tier, func(t *testing.T) {
			t.Parallel()
			if err := ValidateResourceBudget(design, tier); err != nil {
				t.Fatalf("expected %s tier to pass: %v", tier, err)
			}
		})
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
	// Proxy is sized one tier above redis-internal (default cache small → proxy medium).
	assertQuantityEqual(t, ProxyCacheOverhead(design).CPU, "250m", "proxy extra cpu")
	assertQuantityEqual(t, quota.CPU, "650m", "quota cpu with overhead")
}

func TestProxyCacheOverhead_mediumProfile(t *testing.T) {
	t.Parallel()

	design := DeploymentDesign{
		Template: "url-shortener",
		Nodes: []DeploymentNode{
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]interface{}{
					"strategy":        "write-through",
					"resourceProfile": "medium",
				},
			},
		},
	}
	NormalizeDesign(&design)
	assertQuantityEqual(t, ProxyCacheOverhead(design).CPU, "500m", "medium canvas → large proxy")
}

func TestProxyCacheOverhead_explicitSmallProxy(t *testing.T) {
	t.Parallel()

	design := DeploymentDesign{
		Template: "url-shortener",
		Nodes: []DeploymentNode{
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]interface{}{
					"strategy":             "write-through",
					"resourceProfile":      "medium",
					"proxyResourceProfile": "small",
				},
			},
		},
	}
	NormalizeDesign(&design)
	assertQuantityEqual(t, ProxyCacheOverhead(design).CPU, "250m", "explicit small proxy")
}

func TestSidecarOverhead_cacheAsideOnlyOnRedis(t *testing.T) {
	t.Parallel()

	design := urlShortenerDefaultDesign()
	got := SidecarOverhead(design)
	assertQuantityEqual(t, got.CPU, "50m", "exporter cpu")
}

func TestAutopilotAdmissionOverhead_urlShortenerDefault(t *testing.T) {
	t.Parallel()

	design := urlShortenerDefaultDesign()
	got := autopilotAdmissionOverhead(design, "explorer")
	assertQuantityEqual(t, got.Memory, "768Mi", "six sub-512Mi containers × 128Mi")
}

func TestQuotaResourceLimits_scaledLargeEngineerIncludesSlack(t *testing.T) {
	t.Parallel()

	design := urlShortenerScaledLargeDesign()
	user := SumDesignResources(design, "engineer")
	sidecar := SidecarOverhead(design)
	slack := autopilotAdmissionOverhead(design, "engineer")
	quota := QuotaResourceLimits(design, "engineer")

	assertQuantityEqual(t, slack.Memory, "384Mi", "small cache main/exporter + postgres exporter slack")
	minMem := user.Memory.DeepCopy()
	minMem.Add(sidecar.Memory)
	minMem.Add(slack.Memory)
	if quota.Memory.Cmp(minMem) < 0 {
		t.Fatalf("quota memory %v below requested+sidecar+slack %v", quota.Memory, minMem)
	}
}

func TestValidateResourceBudget_unaffectedByQuotaSlack(t *testing.T) {
	t.Parallel()

	if err := ValidateResourceBudget(urlShortenerLiteDesign(), "explorer"); err != nil {
		t.Fatalf("explorer lite: %v", err)
	}
	if err := ValidateResourceBudget(urlShortenerDefaultDesign(), "engineer"); err != nil {
		t.Fatalf("engineer default: %v", err)
	}
	err := ValidateResourceBudget(urlShortenerScaledLargeDesign(), "explorer")
	if err == nil {
		t.Fatal("expected scaled large design to fail explorer tier")
	}
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

func urlShortenerAppDBDesign() DeploymentDesign {
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
			{ID: "db-1", Type: "db", Name: "PostgreSQL"},
		},
	}
	NormalizeDesign(&d)
	return d
}

func socialFeedAsyncDesign() DeploymentDesign {
	d := DeploymentDesign{
		Template:  "social-feed",
		Archetype: "social-feed",
		Nodes: []DeploymentNode{
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "App server",
				Config: map[string]interface{}{
					"replicas":     1,
					"instanceSize": "small",
				},
			},
			{ID: "mq-1", Type: "mq", Name: "Message Queue"},
			{
				ID:   "worker-1",
				Type: "worker",
				Name: "Fan-out Worker",
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
					"strategy":      "timeline-store",
					"cacheStrategy": "timeline-store",
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
