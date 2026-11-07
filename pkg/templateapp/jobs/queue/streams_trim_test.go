package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestStreams_TrimKeepsPending(t *testing.T) {
	t.Parallel()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	q, err := NewStreams(StreamsOptions{
		Client:       client,
		StreamKey:    "fanout",
		TrimInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := q.Publish(ctx, []byte("m")); err != nil {
			t.Fatal(err)
		}
	}

	consumer := "c1"
	msgs, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    streamsConsumerGroup,
		Consumer: consumer,
		Streams:  []string{"fanout", ">"},
		Count:    5,
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	// Ack the first two only; three remain pending in the group.
	ackIDs := []string{msgs[0].Messages[0].ID, msgs[0].Messages[1].ID}
	if _, err := client.XAck(ctx, "fanout", streamsConsumerGroup, ackIDs...).Result(); err != nil {
		t.Fatal(err)
	}

	q.trimStreamNow(ctx)

	len, err := client.XLen(ctx, "fanout").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len < 3 {
		t.Fatalf("trim removed pending entries: xlen=%d", len)
	}
}

func TestStreams_TrimAfterAllAcked(t *testing.T) {
	t.Parallel()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	q, err := NewStreams(StreamsOptions{
		Client:       client,
		StreamKey:    "fanout",
		TrimInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 8; i++ {
		if err := q.Publish(ctx, []byte("m")); err != nil {
			t.Fatal(err)
		}
	}

	consumer := "c1"
	msgs, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    streamsConsumerGroup,
		Consumer: consumer,
		Streams:  []string{"fanout", ">"},
		Count:    8,
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(msgs[0].Messages))
	for _, m := range msgs[0].Messages {
		ids = append(ids, m.ID)
	}
	if _, err := client.XAck(ctx, "fanout", streamsConsumerGroup, ids...).Result(); err != nil {
		t.Fatal(err)
	}

	q.trimStreamNow(ctx)

	length, err := client.XLen(ctx, "fanout").Result()
	if err != nil {
		t.Fatal(err)
	}
	if length > 2 {
		t.Fatalf("expected trim to drop acked ids, xlen=%d", length)
	}
}
