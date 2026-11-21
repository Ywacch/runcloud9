package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/placement"
)

func TestRedisQueue_UsesGroupLag(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "redis_stream_group_lag"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("12"))
		case strings.Contains(q, "redis_stream_group_messages_pending"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("3"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	topo := placement.Topology{
		Nodes: []placement.Node{{
			NodeID: "cache-wq",
			Engine: "redis",
			Members: []placement.Member{{
				Role: "primary", AppLabel: "work-queue", Container: "redis",
			}},
		}},
		Jobs: []placement.Job{{
			Job: "work_queue", Component: "mq", NodeID: "cache-wq", Engine: "redis",
		}},
	}
	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	cm := ComponentMetrics{Component: "mq"}
	_ = c.fillWorkQueueRedisStreams(context.Background(), "ns1", "ecommerce", topo.Jobs[0], &topo, &cm)
	if cm.MessagesReady != 12 {
		t.Fatalf("messagesReady=%v want 12", cm.MessagesReady)
	}
	if cm.MessagesUnacked != 3 {
		t.Fatalf("messagesUnacked=%v want 3", cm.MessagesUnacked)
	}
	if cm.QueueDepth != 15 {
		t.Fatalf("queueDepth=%v want 15", cm.QueueDepth)
	}
}

func TestRedisQueue_MissingLagIsGap(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	topo := placement.Topology{
		Nodes: []placement.Node{{
			NodeID: "cache-wq",
			Engine: "redis",
			Members: []placement.Member{{
				Role: "primary", AppLabel: "work-queue", Container: "redis",
			}},
		}},
		Jobs: []placement.Job{{
			Job: "work_queue", Component: "mq", NodeID: "cache-wq", Engine: "redis",
		}},
	}
	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	cm := ComponentMetrics{Component: "mq"}
	gaps := c.fillWorkQueueRedisStreams(context.Background(), "ns1", "ecommerce", topo.Jobs[0], &topo, &cm)
	if cm.MessagesReady != 0 || cm.QueueDepth != 0 {
		t.Fatalf("expected absent queue fields, got ready=%v depth=%v", cm.MessagesReady, cm.QueueDepth)
	}
	var found bool
	for _, g := range gaps {
		if g.Field == "messagesReady" && g.Availability == "not_built" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected messagesReady gap, got %+v", gaps)
	}
}

func TestCachePromQL_FiltersByApp_TwoRedisPods(t *testing.T) {
	t.Parallel()
	var sawRedis, sawWorkQueue bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "redis_keyspace_hits_total") {
			if strings.Contains(q, `app="redis"`) {
				sawRedis = true
				_ = json.NewEncoder(w).Encode(promVectorJSON("100"))
				return
			}
			if strings.Contains(q, `app="work-queue"`) {
				sawWorkQueue = true
			}
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	topo := placement.Topology{
		Jobs: []placement.Job{
			{Job: "hot_cache", Component: "cache", NodeID: "cache-1", Engine: "redis"},
			{Job: "work_queue", Component: "mq", NodeID: "cache-wq", Engine: "redis"},
		},
		Nodes: []placement.Node{
			{NodeID: "cache-1", Members: []placement.Member{{Role: "primary", AppLabel: "redis", Container: "redis"}}},
			{NodeID: "cache-wq", Members: []placement.Member{{Role: "primary", AppLabel: "work-queue", Container: "redis"}}},
		},
	}
	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	c.SetPlacement("ns1", topo)
	dm, err := c.Collect(context.Background(), "ns1", "ecommerce", TopologyFlags{HasCache: true, CacheEngine: "redis", HasMQ: true})
	if err != nil {
		t.Fatal(err)
	}
	if !sawRedis || sawWorkQueue {
		t.Fatalf("redis=%v work-queue=%v", sawRedis, sawWorkQueue)
	}
	if dm.Components["cache"].RedisKeyspaceHits != 100 {
		t.Fatalf("cache hits=%v", dm.Components["cache"].RedisKeyspaceHits)
	}
}

func TestCachePromQL_FiltersByApp(t *testing.T) {
	t.Parallel()
	if got := promAppSelector("ns1", "redis"); got != `namespace="ns1",app="redis"` {
		t.Fatalf("selector=%q", got)
	}
	topo := placement.Topology{
		Jobs: []placement.Job{{Job: "hot_cache", Component: "cache", NodeID: "c1", Engine: "redis"}},
		Nodes: []placement.Node{{
			NodeID:  "c1",
			Members: []placement.Member{{Role: "primary", AppLabel: "redis", Container: "redis"}},
		}},
	}
	if got := jobComponentAppLabel(&topo, "cache"); got != "redis" {
		t.Fatalf("appLabel=%q", got)
	}
}
