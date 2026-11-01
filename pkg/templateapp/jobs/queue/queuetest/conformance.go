package queuetest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue"
)

var errRedeliverTest = errors.New("redeliver test failure")

// Factory returns a fresh queue and cleanup.
type Factory func(t *testing.T) (queue.Queue, func())

// RunRedeliverConformance verifies at-least-once delivery when the handler fails once.
func RunRedeliverConformance(t *testing.T, newQueue Factory) {
	t.Helper()
	ctx := context.Background()
	body := []byte("queue-redeliver-conformance")
	q, cleanup := newQueue(t)
	defer cleanup()

	if err := q.Publish(ctx, body); err != nil {
		t.Fatalf("publish: %v", err)
	}

	firstDone := make(chan struct{}, 1)
	consumerCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- q.Consume(consumerCtx, func(_ context.Context, b []byte) error {
			if bytes.Equal(b, body) {
				select {
				case firstDone <- struct{}{}:
				default:
				}
				return errRedeliverTest
			}
			return nil
		})
	}()

	select {
	case <-firstDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}
	cancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for consumer stop")
	}

	secondCtx, cancelSecond := context.WithTimeout(ctx, 10*time.Second)
	defer cancelSecond()
	gotSecond := make(chan struct{}, 1)
	go func() {
		_ = q.Consume(secondCtx, func(_ context.Context, b []byte) error {
			if bytes.Equal(b, body) {
				gotSecond <- struct{}{}
				return nil
			}
			return nil
		})
	}()

	select {
	case <-gotSecond:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for redelivered message")
	}
}
