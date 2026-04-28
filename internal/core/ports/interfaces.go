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

// TelemetryRepository defines the interface for persisting telemetry to PostgreSQL.
type TelemetryRepository interface {
	// InsertBatchIdempotent inserts a batch of telemetry rows idempotently.
	// Returns the number of rows actually inserted (excluding duplicates).
	InsertBatchIdempotent(ctx context.Context, rows []domain.EnrichedTelemetry) (int, error)
	// GetLatest returns the most recent telemetry for a device.
	GetLatest(ctx context.Context, deviceID string) (*domain.EnrichedTelemetry, error)
	// QueryRange returns telemetry within a time range.
	QueryRange(ctx context.Context, q domain.TelemetryRangeQuery) ([]domain.EnrichedTelemetry, error)
	// Ping checks database connectivity.
	Ping(ctx context.Context) error
}

// ProfileRegistry defines the interface for device profile matching and transformation.
type ProfileRegistry interface {
	// Match finds the best matching profile for a payload.
	Match(payload domain.RawTelemetryPayload) (*domain.DeviceProfile, error)
	// Transform converts raw values to metrics using the matched profile.
	Transform(payload domain.RawTelemetryPayload, profile *domain.DeviceProfile) (map[string]any, error)
}

// MQTTSubscriber defines the interface for MQTT message consumption.
type MQTTSubscriber interface {
	// Subscribe connects to the broker and starts consuming messages.
	Subscribe(ctx context.Context, handler func(msg domain.RawTelemetryPayload) error) error
	// IsConnected returns true if the client is connected.
	IsConnected() bool
	// Close disconnects from the broker.
	Close() error
}

// MetricsRecorder defines the interface for recording operational metrics.
type MetricsRecorder interface {
	IncMQTTReceived()
	IncMQTTRejected(reason string)
	IncRedisXAddError()
	IncWorkerProcessed()
	IncWorkerDuplicate()
	IncWorkerFailed(reason string)
	IncDeadlettered()
	ObserveBatchInsertDuration(seconds float64)
}

// Clock defines the interface for time operations.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// Since returns the duration since the given time.
	Since(time.Time) time.Duration
}
