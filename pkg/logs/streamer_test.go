package logs

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDiscoverPodLogStreams_CacheAside(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis-1",
				Namespace: "ns1",
				Labels:    map[string]string{"app": "redis", "deployment": "ns1"},
			},
		},
	)

	streams := discoverPodLogStreams(context.Background(), client, "ns1")
	if len(streams) == 0 {
		t.Fatal("expected at least one stream")
	}

	foundCacheRedis := false
	for _, s := range streams {
		if s.component == "cache" && s.podName == "redis-1" && s.containerName == "redis" {
			foundCacheRedis = true
			break
		}
	}
	if !foundCacheRedis {
		t.Fatalf("expected cache redis stream, got %+v", streams)
	}
}

func TestDiscoverPodLogStreams_ProxyCacheTargets(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cache-proxy-1",
				Namespace: "ns1",
				Labels:    map[string]string{"app": "cache-proxy", "deployment": "ns1"},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis-internal-1",
				Namespace: "ns1",
				Labels:    map[string]string{"app": "redis-internal", "deployment": "ns1"},
			},
		},
	)

	streams := discoverPodLogStreams(context.Background(), client, "ns1")
	if len(streams) == 0 {
		t.Fatal("expected at least one stream")
	}

	foundProxy := false
	foundRedisInternal := false
	for _, s := range streams {
		if s.component == "cache" && s.podName == "cache-proxy-1" && s.containerName == "cache-proxy" {
			foundProxy = true
		}
		if s.component == "cache" && s.podName == "redis-internal-1" && s.containerName == "redis" {
			foundRedisInternal = true
		}
	}
	if !foundProxy || !foundRedisInternal {
		t.Fatalf("expected proxy and redis-internal streams, got %+v", streams)
	}
}

func TestDiscoverPodLogStreams_DeduplicatesPodContainer(t *testing.T) {
	original := componentLogTargets
	componentLogTargets = map[string][]componentLogTarget{
		"cache": {
			{labelSelector: "app=redis", containerName: "redis"},
			{labelSelector: "app=redis", containerName: "redis"},
		},
	}
	defer func() { componentLogTargets = original }()

	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis-1",
				Namespace: "ns1",
				Labels:    map[string]string{"app": "redis", "deployment": "ns1"},
			},
		},
	)

	streams := discoverPodLogStreams(context.Background(), client, "ns1")
	if len(streams) != 1 {
		t.Fatalf("expected one deduplicated stream, got %d (%+v)", len(streams), streams)
	}
}

func TestDiscoverPodLogStreams_PostgresPrimaryAndReplica(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "postgres-abc",
				Namespace: "ns1",
				Labels:    map[string]string{"app": "postgres", "deployment": "ns1"},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "postgres-replica-xyz",
				Namespace: "ns1",
				Labels:    map[string]string{"app": "postgres-replica", "deployment": "ns1"},
			},
		},
	)

	streams := discoverPodLogStreams(context.Background(), client, "ns1")
	foundPrimary := false
	foundReplica := false
	for _, s := range streams {
		if s.component == "db" && s.podName == "postgres-abc" && s.containerName == "postgres" {
			foundPrimary = true
		}
		if s.component == "db" && s.podName == "postgres-replica-xyz" && s.containerName == "postgres-replica" {
			foundReplica = true
		}
	}
	if !foundPrimary || !foundReplica {
		t.Fatalf("expected postgres and postgres-replica db streams, got %+v", streams)
	}
}

func TestParseLine_setsPod(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, line, component, pod string
	}{
		{name: "json", line: `{"msg":"request handled","level":"info"}`, component: "app-server", pod: "app-server-abc"},
		{name: "plaintext", line: "LOG: ready", component: "db", pod: "postgres-0"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := parseLine(tc.line, tc.component, tc.pod)
			if got.Pod != tc.pod {
				t.Fatalf("Pod = %q want %q", got.Pod, tc.pod)
			}
			if got.Component != tc.component {
				t.Fatalf("Component = %q want %q", got.Component, tc.component)
			}
		})
	}
}
