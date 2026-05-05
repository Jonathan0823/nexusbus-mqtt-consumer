package ports

import (
	"context"

	"modbus-mqtt-consumer/internal/core/domain"
)

// TelemetryService defines the interface for telemetry query operations.
type TelemetryService interface {
	// QueryDeviceTelemetry returns telemetry data for a device within a time range.
	QueryDeviceTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error)
}