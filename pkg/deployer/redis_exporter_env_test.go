package deployer

import "testing"

func TestRedisExporterEnv_WorkQueueStreams(t *testing.T) {
	t.Parallel()
	cfg := DeployConfig{
		Namespace: "lab",
		Node:      DeploymentNode{ID: "cache-wq", Type: "cache", Config: map[string]any{"engine": "redis"}},
		Design: DeploymentDesign{
			Template: "ecommerce",
			Nodes: []DeploymentNode{
				{ID: "cache-wq", Type: "cache", Config: map[string]any{"engine": "redis"}},
			},
			JobBindings: map[string]string{"work_queue": "cache-wq"},
		},
	}
	env := redisExporterEnv(cfg)
	var streams string
	for _, e := range env {
		if e.Name == "REDIS_EXPORTER_CHECK_SINGLE_STREAMS" {
			streams = e.Value
		}
	}
	if streams != "orders" {
		t.Fatalf("streams env=%q want orders", streams)
	}
}
