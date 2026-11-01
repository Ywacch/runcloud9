package deployer

import (
	"strings"
	"testing"
)

func socialFeedRedisQueueDesign(bind bool, workerToCache bool) DeploymentDesign {
	conns := []DeploymentConn{
		{SourceID: "app", TargetID: "db"},
		{SourceID: "app", TargetID: "cache"},
		{SourceID: "worker", TargetID: "db"},
	}
	if workerToCache {
		conns = append(conns, DeploymentConn{SourceID: "worker", TargetID: "cache"})
	}
	d := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "app", Type: "app-server", Name: "App"},
			{ID: "db", Type: "db", Name: "DB"},
			{ID: "cache", Type: "cache", Name: "Redis"},
			{ID: "worker", Type: "worker", Name: "Fan-out Worker"},
		},
		Connections: conns,
	}
	if bind {
		d.JobBindings = map[string]string{"work_queue": "cache"}
	}
	return d
}

func TestWorkQueueOnCacheSatisfiesWorker(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		design     DeploymentDesign
		wantErr    string
		wantAssess string
	}{
		{
			name:   "bound cache with worker edge",
			design: socialFeedRedisQueueDesign(true, true),
		},
		{
			name:       "bound cache without worker edge",
			design:     socialFeedRedisQueueDesign(true, false),
			wantErr:    "hosts the work queue",
			wantAssess: "hosts the work queue",
		},
		{
			name:       "worker without mq and without binding",
			design:     socialFeedRedisQueueDesign(false, true),
			wantErr:    "add worker -> mq edge",
			wantAssess: "message queue",
		},
		{
			name: "binding points at a missing node",
			design: func() DeploymentDesign {
				d := socialFeedRedisQueueDesign(false, true)
				d.JobBindings = map[string]string{"work_queue": "nope"}
				return d
			}(),
			wantErr:    "add worker -> mq edge",
			wantAssess: "message queue",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateTopology(tt.design)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTopology: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateTopology: got %v, want substring %q", err, tt.wantErr)
			}

			a := AssessTopology(tt.design)
			if tt.wantAssess == "" {
				if a.Verdict != VerdictFull {
					t.Fatalf("AssessTopology: %q (%s)", a.Verdict, a.Explanation)
				}
				return
			}
			if a.Verdict != VerdictInvalid || !strings.Contains(a.Explanation, tt.wantAssess) {
				t.Fatalf("AssessTopology: %q (%s), want substring %q", a.Verdict, a.Explanation, tt.wantAssess)
			}
		})
	}
}
