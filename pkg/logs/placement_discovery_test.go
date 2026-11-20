package logs

import (
	"context"
	"testing"

	"github.com/ywacch/cloud9/pkg/placement"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDiscoverPodLogStreams_PlacementWorkQueue(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "work-queue-0",
				Namespace: "ns1",
				Labels:    map[string]string{"app": "work-queue"},
			},
		},
	)
	topo := placement.Topology{
		Nodes: []placement.Node{{
			NodeID:   "cache-wq",
			NodeType: "cache",
			Members: []placement.Member{{
				Role: "primary", AppLabel: "work-queue", Container: "redis",
			}},
			Jobs: []string{"work_queue"},
		}},
		Jobs: []placement.Job{{
			Job: "work_queue", Component: "mq", NodeID: "cache-wq", Engine: "redis",
		}},
	}
	streams := discoverPodLogStreams(context.Background(), client, "ns1", &topo)
	if len(streams) != 1 {
		t.Fatalf("streams: %+v", streams)
	}
	s := streams[0]
	if s.component != "mq" || s.nodeID != "cache-wq" || s.podName != "work-queue-0" {
		t.Fatalf("got %+v", s)
	}
}

func TestParseLine_setsNodeID(t *testing.T) {
	t.Parallel()
	got := parseLine("LOG: ok", "cache", "node-abc", "redis-0")
	if got.NodeID != "node-abc" {
		t.Fatalf("NodeID = %q", got.NodeID)
	}
}
