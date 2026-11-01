// Package queue provides at-least-once work queues for template jobs.
//
// Consume calls the handler for each message. The implementation acknowledges
// the message when the handler returns nil. When the handler returns an error,
// the message is not acknowledged and may be delivered again. Ordering is not
// guaranteed.
//
// After five failed delivery attempts for the same message, the implementation
// increments queue_poison_total and then acknowledges the message so it cannot
// block the rest of the group.
package queue

import "context"

// Queue publishes and consumes opaque job payloads for a single logical queue name.
type Queue interface {
	Publish(ctx context.Context, body []byte) error
	Consume(ctx context.Context, h func(ctx context.Context, body []byte) error) error
	Engine() string
}

const maxDeliveryAttempts = 5
