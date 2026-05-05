package ports

import (
	"context"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
)

// MessageBuffer defines the interface for buffering messages in Redis Streams.
type MessageBuffer interface {
	// Add writes a message to the stream.
	Add(ctx context.Context, msg domain.RawTelemetryMessage) error
	// ReadBatch reads a batch of messages from the stream.
	ReadBatch(ctx context.Context, max int) ([]domain.BufferedMessage, error)
	// Ack acknowledges processed messages.
	Ack(ctx context.Context, ids []string) error
	// ClaimStale claims messages that have been pending too long.
	ClaimStale(ctx context.Context, minIdle time.Duration, max int) ([]domain.BufferedMessage, error)
	// IncrementRetry increments the retry counter for a message.
	IncrementRetry(ctx context.Context, id string) (int, error)
	// ResetRetry clears retry counters for processed message IDs.
	ResetRetry(ctx context.Context, ids []string) error
	// Deadletter moves a failed message to the deadletter stream.
	Deadletter(ctx context.Context, msg domain.BufferedMessage, reason string, retryCount int) error
	// Length returns the number of messages in the stream.
	Length(ctx context.Context) (int64, error)
}