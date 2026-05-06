package ports

import (
	"context"

	"modbus-mqtt-consumer/internal/core/domain"
)

// MQTTSubscriber defines the interface for MQTT message consumption.
type MQTTSubscriber interface {
	// Subscribe connects to the broker and starts consuming messages.
	Subscribe(ctx context.Context, handler func(msg domain.RawTelemetryPayload) error) error
	// IsConnected returns true if the client is connected.
	IsConnected() bool
	// Close disconnects from the broker.
	Close() error
}