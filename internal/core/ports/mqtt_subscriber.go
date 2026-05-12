package ports

import (
	"context"

	"modbus-mqtt-consumer/internal/core/domain"
)

// MQTTSubscriber defines the interface for MQTT message consumption.
type MQTTSubscriber interface {
	// Subscribe connects to the broker and starts consuming messages.
	Subscribe(ctx context.Context, handler func(msg domain.RawTelemetryPayload) error) error
	// IsReady returns true if the client is connected and subscribed to the topic.
	IsReady() bool
	// Close disconnects from the broker.
	Close() error
}
