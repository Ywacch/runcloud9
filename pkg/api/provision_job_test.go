package api

import (
	"testing"
	"time"
)

func TestProvisionOrchestrationTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		deployerEnv string
		want        time.Duration
	}{
		{name: "local default", deployerEnv: "", want: 2 * time.Minute},
		{name: "local explicit", deployerEnv: "local", want: 2 * time.Minute},
		{name: "k8s", deployerEnv: "k8s", want: 5 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := provisionOrchestrationTimeout(tt.deployerEnv)
			if got != tt.want {
				t.Fatalf("provisionOrchestrationTimeout(%q)=%s want %s", tt.deployerEnv, got, tt.want)
			}
		})
	}
}
