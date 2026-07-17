package deployer

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestDBResources_matchesSmallProfile(t *testing.T) {
	t.Parallel()

	got := DBResources()
	want := ResourceProfile("small")
	assertResourceRequirementsEqual(t, got, want)
}

func TestComponentDefaultResources_mqAndLb(t *testing.T) {
	t.Parallel()

	mq := ComponentDefaultResources("mq")
	if mq.Limits.Cpu().String() != "200m" {
		t.Fatalf("mq cpu: got %s want 200m", mq.Limits.Cpu())
	}

	lb := ComponentDefaultResources("lb")
	if lb.Limits.Cpu().String() != "250m" {
		t.Fatalf("lb cpu: got %s want 250m", lb.Limits.Cpu())
	}
	if lb.Limits.Memory().String() != "128Mi" {
		t.Fatalf("lb memory: got %s want 128Mi", lb.Limits.Memory())
	}
}

func TestResolveNodeResources_profileOverridesComponentDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		nodeType string
		config   map[string]interface{}
		wantCPU  string
	}{
		{
			name:     "db large profile",
			nodeType: "db",
			config:   map[string]interface{}{"resourceProfile": "large"},
			wantCPU:  "500m",
		},
		{
			name:     "mq default without profile",
			nodeType: "mq",
			config:   map[string]interface{}{},
			wantCPU:  "200m",
		},
		{
			name:     "mq medium profile",
			nodeType: "mq",
			config:   map[string]interface{}{"resourceProfile": "medium"},
			wantCPU:  "250m",
		},
		{
			name:     "lb small profile",
			nodeType: "lb",
			config:   map[string]interface{}{"resourceProfile": "small"},
			wantCPU:  "125m",
		},
		{
			name:     "app-server default",
			nodeType: "app-server",
			config:   map[string]interface{}{},
			wantCPU:  "125m",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ResolveNodeResources(tt.nodeType, tt.config)
			if got.Limits.Cpu().String() != tt.wantCPU {
				t.Fatalf("cpu limit: got %s want %s", got.Limits.Cpu(), tt.wantCPU)
			}
		})
	}
}

func TestCacheProxyResources_oneTierAboveRedis(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		config  map[string]interface{}
		wantCPU string
		wantMem string
	}{
		{name: "default/small", config: nil, wantCPU: "250m", wantMem: "512Mi"},
		{name: "small", config: map[string]interface{}{"resourceProfile": "small"}, wantCPU: "250m", wantMem: "512Mi"},
		{name: "medium", config: map[string]interface{}{"resourceProfile": "medium"}, wantCPU: "500m", wantMem: "1Gi"},
		{name: "large", config: map[string]interface{}{"resourceProfile": "large"}, wantCPU: "750m", wantMem: "1536Mi"},
		{name: "alpha", config: map[string]interface{}{"resourceProfile": "alpha"}, wantCPU: "250m", wantMem: "512Mi"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CacheProxyResources(tc.config)
			if got.Limits.Cpu().String() != tc.wantCPU {
				t.Fatalf("cpu: got %s want %s", got.Limits.Cpu(), tc.wantCPU)
			}
			if got.Limits.Memory().String() != tc.wantMem {
				t.Fatalf("memory: got %s want %s", got.Limits.Memory(), tc.wantMem)
			}
			redis := ResolveNodeResources("cache", tc.config)
			if redis.Limits.Cpu().Cmp(*got.Limits.Cpu()) >= 0 {
				t.Fatalf("proxy cpu %s should exceed redis-internal %s", got.Limits.Cpu(), redis.Limits.Cpu())
			}
		})
	}
}

func TestCacheProxyResources_explicitProxyProfile(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		config  map[string]interface{}
		wantCPU string
		wantMem string
	}{
		{
			name: "small proxy with medium redis",
			config: map[string]interface{}{
				"resourceProfile":      "medium",
				"proxyResourceProfile": "small",
			},
			wantCPU: "250m",
			wantMem: "512Mi",
		},
		{
			name: "large proxy with small redis",
			config: map[string]interface{}{
				"resourceProfile":      "small",
				"proxyResourceProfile": "large",
			},
			wantCPU: "750m",
			wantMem: "1536Mi",
		},
		{
			name: "medium proxy overrides medium redis default",
			config: map[string]interface{}{
				"resourceProfile":      "medium",
				"proxyResourceProfile": "medium",
			},
			wantCPU: "500m",
			wantMem: "1Gi",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CacheProxyResources(tc.config)
			if got.Limits.Cpu().String() != tc.wantCPU {
				t.Fatalf("cpu: got %s want %s", got.Limits.Cpu(), tc.wantCPU)
			}
			if got.Limits.Memory().String() != tc.wantMem {
				t.Fatalf("memory: got %s want %s", got.Limits.Memory(), tc.wantMem)
			}
		})
	}
}

func TestDefaultProxyResourceProfile(t *testing.T) {
	t.Parallel()
	cases := []struct {
		redis string
		want  string
	}{
		{"small", "small"},
		{"medium", "medium"},
		{"large", "large"},
		{"alpha", "small"},
		{"", "small"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.redis, func(t *testing.T) {
			t.Parallel()
			if got := DefaultProxyResourceProfile(tc.redis); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestResourceCatalogResponse_alphaMatchesSmall(t *testing.T) {
	t.Parallel()

	resp := NewResourceCatalogResponse()
	small, ok := resp.Profiles["small"]
	if !ok {
		t.Fatal("missing small profile")
	}
	alpha, ok := resp.Profiles["alpha"]
	if !ok {
		t.Fatal("missing alpha profile")
	}
	if small != alpha {
		t.Fatalf("alpha should match small: alpha=%+v small=%+v", alpha, small)
	}
	if resp.Components["db"].CPU != "125m" {
		t.Fatalf("db cpu: got %s", resp.Components["db"].CPU)
	}
}

func assertResourceRequirementsEqual(t *testing.T, got, want corev1.ResourceRequirements) {
	t.Helper()

	if got.Limits.Cpu().Cmp(*want.Limits.Cpu()) != 0 {
		t.Fatalf("cpu limit: got %s want %s", got.Limits.Cpu(), want.Limits.Cpu())
	}
	if got.Limits.Memory().Cmp(*want.Limits.Memory()) != 0 {
		t.Fatalf("memory limit: got %s want %s", got.Limits.Memory(), want.Limits.Memory())
	}
	if got.Requests.Cpu().String() != "125m" {
		t.Fatalf("cpu request: got %s want 125m", got.Requests.Cpu())
	}
	if got.Requests.Memory().String() != "256Mi" {
		t.Fatalf("memory request: got %s want 256Mi", got.Requests.Memory())
	}
}
