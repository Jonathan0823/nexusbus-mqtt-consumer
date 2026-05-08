package ports

import (
	"context"

	"modbus-mqtt-consumer/internal/core/domain"
)

// TelemetryService defines the interface for telemetry query operations.
type TelemetryService interface {
	// QueryDeviceTelemetry returns telemetry data for a device within a time range.
	QueryDeviceTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error)
	// QueryDeviceChart returns chart series for a device within a time range.
	QueryDeviceChart(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error)
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
	// QueryTelemetry returns telemetry for dashboard queries.
	QueryTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error)
	// StreamTelemetry streams telemetry rows for a query (for large range queries).
	// The callback is called for each row. Returns error if any.
	StreamTelemetry(ctx context.Context, q domain.TelemetryQuery, fn func(domain.EnrichedTelemetry) error) error
	// Ping checks database connectivity.
	Ping(ctx context.Context) error
}

