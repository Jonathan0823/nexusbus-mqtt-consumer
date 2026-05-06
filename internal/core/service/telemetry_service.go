package service

import (
	"context"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/ports"
)

// TelemetryServiceImpl implements ports.TelemetryService.
type TelemetryServiceImpl struct {
	repo ports.TelemetryRepository
}

// NewTelemetryService creates a new telemetry service.
func NewTelemetryService(repo ports.TelemetryRepository) *TelemetryServiceImpl {
	return &TelemetryServiceImpl{
		repo: repo,
	}
}

// QueryDeviceTelemetry returns telemetry data for a device within a time range.
func (s *TelemetryServiceImpl) QueryDeviceTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error) {
	return s.repo.QueryTelemetry(ctx, q)
}