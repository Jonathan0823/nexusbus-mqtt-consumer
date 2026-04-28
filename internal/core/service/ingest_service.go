package service

import (
	"context"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/ports"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// IngestService handles MQTT message ingestion.
type IngestService struct {
	buffer ports.MessageBuffer
	logger *logging.Logger
}

// NewIngestService creates a new ingest service.
func NewIngestService(buffer ports.MessageBuffer, logger *logging.Logger) *IngestService {
	return &IngestService{
		buffer: buffer,
		logger: logger,
	}
}

// Handle processes an incoming MQTT payload.
func (s *IngestService) Handle(ctx context.Context, payload domain.RawTelemetryPayload) error {
	// Validate mandatory fields
	if err := payload.IsValid(); err != nil {
		s.logger.Error("payload validation failed", "error", err, "device_id", payload.DeviceID)
		return err
	}

	// Create the message to buffer
	msg := domain.RawTelemetryMessage{
		Payload:    payload,
		ReceivedAt: time.Now().UTC(),
	}

	// Write to Redis Stream
	if err := s.buffer.Add(ctx, msg); err != nil {
		s.logger.Error("buffer add failed", "error", err, "device_id", payload.DeviceID)
		return err
	}

	s.logger.Debug("ingested message", "device_id", payload.DeviceID)
	return nil
}
