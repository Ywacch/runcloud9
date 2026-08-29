package deployer

import "testing"

func TestWaitCodeFromKubeReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reason string
		want   string
	}{
		{reason: "ImagePullBackOff", want: WaitCodePullingImage},
		{reason: "ErrImagePull", want: WaitCodePullingImage},
		{reason: "PullImage", want: WaitCodePullingImage},
		{reason: "CrashLoopBackOff", want: WaitCodeCrashLoop},
		{reason: "CreateContainerConfigError", want: WaitCodeConfigError},
		{reason: "InvalidImageName", want: WaitCodeConfigError},
		{reason: "ContainerCreating", want: WaitCodeStarting},
		{reason: "PodInitializing", want: WaitCodeStarting},
		{reason: "SomethingElse", want: WaitCodeStarting},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.reason, func(t *testing.T) {
			t.Parallel()
			if got := WaitCodeFromKubeReason(tc.reason); got != tc.want {
				t.Fatalf("WaitCodeFromKubeReason(%q) = %q, want %q", tc.reason, got, tc.want)
			}
		})
	}
}

func TestComputeProvisionDetailCode(t *testing.T) {
	t.Parallel()

	replicasPulling := map[string][]PodWaitProgress{
		"app-1": {{Ordinal: 1, Ready: false, WaitCode: WaitCodePullingImage}},
	}

	tests := []struct {
		name       string
		seeding    string
		replicas   map[string][]PodWaitProgress
		phase      string
		failDetail string
		want       string
	}{
		{name: "seeding", seeding: "seeding", want: "loading_sample_data"},
		{name: "pulling", replicas: replicasPulling, want: WaitCodePullingImage},
		{name: "deployFailTimeout", failDetail: WaitCodeTimeout, want: WaitCodeTimeout},
		{name: "default", want: "layer_deploying"},
		{name: "failedPhase", phase: "failed", want: "failed"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ComputeProvisionDetailCode(tc.seeding, tc.replicas, tc.phase, tc.failDetail)
			if got != tc.want {
				t.Fatalf("ComputeProvisionDetailCode() = %q, want %q", got, tc.want)
			}
		})
	}
}
