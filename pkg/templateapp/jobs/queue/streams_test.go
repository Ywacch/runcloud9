package queue_test

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue/queuetest"
)

func TestQueueConformance_Streams_Redeliver(t *testing.T) {
	t.Parallel()
	queuetest.RunRedeliverConformance(t, func(t *testing.T) (queue.Queue, func()) {
		t.Helper()
		srv, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		q, err := queue.NewStreams(queue.StreamsOptions{
			Client:           client,
			StreamKey:        "fanout",
			AutoClaimMinIdle: time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		return q, func() {
			_ = client.Close()
			srv.Close()
		}
	})
}
