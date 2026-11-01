package deployer

import (
	"context"
	"strings"
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestParseAutoscaleConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  map[string]any
		want    *AutoscaleConfig
		wantErr string
	}{
		{
			name:   "missing",
			config: map[string]any{"replicas": 1},
			want:   nil,
		},
		{
			name: "valid",
			config: map[string]any{
				"autoscale": map[string]any{
					"min":              1,
					"max":              3,
					"targetCPUPercent": 70,
				},
			},
			want: &AutoscaleConfig{Min: 1, Max: 3, TargetCPUPercent: 70},
		},
		{
			name: "min above max",
			config: map[string]any{
				"autoscale": map[string]any{
					"min":              3,
					"max":              1,
					"targetCPUPercent": 70,
				},
			},
			wantErr: "autoscale.max must be at least autoscale.min",
		},
		{
			name: "percent out of range",
			config: map[string]any{
				"autoscale": map[string]any{
					"min":              1,
					"max":              2,
					"targetCPUPercent": 101,
				},
			},
			wantErr: "autoscale.targetCPUPercent",
		},
		{
			name: "max above replica cap",
			config: map[string]any{
				"autoscale": map[string]any{
					"min":              1,
					"max":              6,
					"targetCPUPercent": 50,
				},
			},
			wantErr: "autoscale.max",
		},
		{
			name: "invalid object type",
			config: map[string]any{
				"autoscale": "bad",
			},
			wantErr: "autoscale must be an object",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseAutoscaleConfig(tc.config)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q should contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAutoscaleConfig: %v", err)
			}
			if tc.want == nil {
				if got != nil {
					t.Fatalf("got %+v want nil", got)
				}
				return
			}
			if got == nil || *got != *tc.want {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

func TestDeployReplicaCount_usesMinWhenAutoscale(t *testing.T) {
	t.Parallel()
	cfg := map[string]any{
		"replicas": 2,
		"autoscale": map[string]any{
			"min":              1,
			"max":              3,
			"targetCPUPercent": 70,
		},
	}
	got, err := deployReplicaCount(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("deployReplicaCount: got %d want 1 (autoscale.min)", got)
	}
}

func TestEnsureCPUHorizontalPodAutoscaler(t *testing.T) {
	t.Parallel()

	cs := fake.NewSimpleClientset()
	ac := AutoscaleConfig{Min: 1, Max: 3, TargetCPUPercent: 70}
	if err := ensureCPUHorizontalPodAutoscaler(context.Background(), cs, "ns-1", "app-server", ac); err != nil {
		t.Fatal(err)
	}
	list, err := cs.AutoscalingV2().HorizontalPodAutoscalers("ns-1").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("hpa count: got %d want 1", len(list.Items))
	}
	hpa := list.Items[0]
	if hpa.Spec.MinReplicas == nil || *hpa.Spec.MinReplicas != 1 {
		t.Fatalf("minReplicas: got %v want 1", hpa.Spec.MinReplicas)
	}
	if hpa.Spec.MaxReplicas != 3 {
		t.Fatalf("maxReplicas: got %d want 3", hpa.Spec.MaxReplicas)
	}
	if len(hpa.Spec.Metrics) != 1 {
		t.Fatalf("metrics: got %d want 1", len(hpa.Spec.Metrics))
	}
	m := hpa.Spec.Metrics[0]
	if m.Type != autoscalingv2.ResourceMetricSourceType || m.Resource == nil {
		t.Fatalf("unexpected metric spec: %+v", m)
	}
	if m.Resource.Name != "cpu" {
		t.Fatalf("metric resource: got %q want cpu", m.Resource.Name)
	}
	if m.Resource.Target.AverageUtilization == nil || *m.Resource.Target.AverageUtilization != 70 {
		t.Fatalf("cpu target: got %v want 70", m.Resource.Target.AverageUtilization)
	}
	if hpa.Spec.ScaleTargetRef.Name != "app-server" {
		t.Fatalf("scale target: got %q want app-server", hpa.Spec.ScaleTargetRef.Name)
	}
	if hpa.Spec.Behavior == nil || hpa.Spec.Behavior.ScaleDown == nil || hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds == nil {
		t.Fatal("expected scale-down stabilization window")
	}
	if got := *hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds; got != hpaScaleDownStabilizationSeconds {
		t.Fatalf("scaleDown stabilization: got %d want %d", got, hpaScaleDownStabilizationSeconds)
	}
}
