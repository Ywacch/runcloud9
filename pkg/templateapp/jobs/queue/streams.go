package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const (
	streamsConsumerGroup = "cloud9-workers"
	streamsAutoClaimMin  = 30 * time.Second
)

// StreamsOptions configures a Redis Streams queue adapter.
type StreamsOptions struct {
	Client           redis.UniversalClient
	StreamKey        string
	Registerer       prometheus.Registerer
	AutoClaimMinIdle time.Duration
}

// Streams implements Queue with XADD, XREADGROUP, XACK, and XAUTOCLAIM.
type Streams struct {
	client       redis.UniversalClient
	stream       string
	reg          prometheus.Registerer
	autoClaimMin time.Duration
}

// NewStreams prepares the stream and consumer group.
func NewStreams(opts StreamsOptions) (*Streams, error) {
	stream := strings.TrimSpace(opts.StreamKey)
	if stream == "" {
		return nil, fmt.Errorf("queue: streams stream key is required")
	}
	if opts.Client == nil {
		return nil, fmt.Errorf("queue: streams redis client is required")
	}
	registerPoisonMetrics(opts.Registerer)
	ctx := context.Background()
	if err := opts.Client.XGroupCreateMkStream(ctx, stream, streamsConsumerGroup, "0").Err(); err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			return nil, fmt.Errorf("queue: streams group create: %w", err)
		}
	}
	minIdle := opts.AutoClaimMinIdle
	if minIdle <= 0 {
		minIdle = streamsAutoClaimMin
	}
	return &Streams{client: opts.Client, stream: stream, reg: opts.Registerer, autoClaimMin: minIdle}, nil
}

// Engine returns "redis".
func (s *Streams) Engine() string {
	return "redis"
}

// Publish appends body to the stream.
func (s *Streams) Publish(ctx context.Context, body []byte) error {
	_, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.stream,
		Values: map[string]interface{}{"body": body},
	}).Result()
	if err != nil {
		return fmt.Errorf("queue: streams xadd: %w", err)
	}
	return nil
}

// Consume reads with XREADGROUP until ctx is cancelled.
func (s *Streams) Consume(ctx context.Context, h func(ctx context.Context, body []byte) error) error {
	consumer := "worker-" + fmt.Sprintf("%d", time.Now().UnixNano())
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.claimPending(ctx, consumer, h); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.autoClaim(ctx, consumer, h); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    streamsConsumerGroup,
			Consumer: consumer,
			Streams:  []string{s.stream, ">"},
			Count:    10,
			Block:    2 * time.Second,
		}).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("queue: streams read: %w", err)
		}
		for _, st := range streams {
			for _, msg := range st.Messages {
				s.handleMessage(ctx, consumer, msg, h)
			}
		}
	}
}

func (s *Streams) claimPending(ctx context.Context, consumer string, h func(ctx context.Context, body []byte) error) error {
	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    streamsConsumerGroup,
		Consumer: consumer,
		Streams:  []string{s.stream, "0"},
		Count:    10,
		Block:    0,
	}).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	for _, st := range streams {
		for _, msg := range st.Messages {
			s.handleMessage(ctx, consumer, msg, h)
		}
	}
	return nil
}

func (s *Streams) autoClaim(ctx context.Context, consumer string, h func(ctx context.Context, body []byte) error) error {
	res, _, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s.stream,
		Group:    streamsConsumerGroup,
		Consumer: consumer,
		MinIdle:  s.autoClaimMin,
		Start:    "0-0",
		Count:    10,
	}).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	for _, msg := range res {
		s.handleMessage(ctx, consumer, msg, h)
	}
	return nil
}

func (s *Streams) handleMessage(ctx context.Context, consumer string, msg redis.XMessage, h func(ctx context.Context, body []byte) error) {
	body, ok := streamBody(msg)
	if !ok {
		incPoison(s.Engine(), s.stream)
		_ = s.client.XAck(ctx, s.stream, streamsConsumerGroup, msg.ID).Err()
		return
	}
	attempts := s.deliveryAttempts(ctx, msg.ID)
	if err := h(ctx, body); err != nil {
		if attempts >= maxDeliveryAttempts {
			incPoison(s.Engine(), s.stream)
			_ = s.client.XAck(ctx, s.stream, streamsConsumerGroup, msg.ID).Err()
		}
		return
	}
	_ = s.client.XAck(ctx, s.stream, streamsConsumerGroup, msg.ID).Err()
}

func streamBody(msg redis.XMessage) ([]byte, bool) {
	raw, ok := msg.Values["body"]
	if !ok {
		return nil, false
	}
	switch v := raw.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	default:
		return nil, false
	}
}

func (s *Streams) deliveryAttempts(ctx context.Context, msgID string) int {
	ext, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.stream,
		Group:  streamsConsumerGroup,
		Start:  msgID,
		End:    msgID,
		Count:  1,
	}).Result()
	if err != nil || len(ext) == 0 {
		return 1
	}
	return int(ext[0].RetryCount) + 1
}

func (s *Streams) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}
