package logs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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

func TestDiscoverPodLogStreams_Memcached(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "memcached-1",
				Namespace: "ns1",
				Labels:    map[string]string{"app": "memcached", "deployment": "ns1"},
			},
		},
	)
	streams := discoverPodLogStreams(context.Background(), client, "ns1")
	found := false
	for _, s := range streams {
		if s.component == "cache" && s.podName == "memcached-1" && s.containerName == "memcached" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected memcached log stream, got %+v", streams)
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

func TestPodLogStreamKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		stream podLogStream
		want   string
	}{
		{
			name:   "primary postgres",
			stream: podLogStream{component: "db", podName: "postgres-abc", containerName: "postgres"},
			want:   "db|postgres-abc|postgres",
		},
		{
			name:   "replica postgres",
			stream: podLogStream{component: "db", podName: "postgres-replica-0", containerName: "postgres-replica"},
			want:   "db|postgres-replica-0|postgres-replica",
		},
		{
			name:   "cache redis",
			stream: podLogStream{component: "cache", podName: "redis-1", containerName: "redis"},
			want:   "cache|redis-1|redis",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.stream.key(); got != tc.want {
				t.Fatalf("key() = %q want %q", got, tc.want)
			}
		})
	}
}

func TestStreamSetTryAdd(t *testing.T) {
	t.Parallel()
	set := newStreamSet()
	primary := podLogStream{component: "db", podName: "postgres-0", containerName: "postgres"}
	replica := podLogStream{component: "db", podName: "postgres-replica-0", containerName: "postgres-replica"}

	if !set.tryAdd(primary) {
		t.Fatal("first add should succeed")
	}
	if set.tryAdd(primary) {
		t.Fatal("duplicate add should fail")
	}
	if !set.tryAdd(replica) {
		t.Fatal("different stream should succeed")
	}
}

func TestConnectTailLines(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		needTail bool
		wantNil  bool
		want     int64
	}{
		{name: "first open", needTail: true, want: logTailLines},
		{name: "reconnect", needTail: false, wantNil: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := connectTailLines(tc.needTail)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("got %v want nil", *got)
				}
				return
			}
			if got == nil {
				t.Fatal("got nil want tail lines")
			}
			if *got != tc.want {
				t.Fatalf("got %d want %d", *got, tc.want)
			}
		})
	}
}

func TestStreamLogs_ClientClose_Stops(t *testing.T) {
	t.Parallel()

	parent := context.Background()
	done := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		defer close(done)
		StreamLogs(parent, conn, fake.NewSimpleClientset(), "ns1")
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	_ = conn.Close()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("StreamLogs kept following after the client closed the socket")
	}
}
