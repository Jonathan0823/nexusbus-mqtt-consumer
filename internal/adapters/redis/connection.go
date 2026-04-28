package redis

import (
	"modbus-mqtt-consumer/internal/platform/logging"
)

// NewConnection creates a Redis stream buffer connection.
// This factory follows the design.md adapter creation pattern.
func NewConnection(cfg Config, logger *logging.Logger) (*StreamBuffer, error) {
	return NewStreamBuffer(cfg, logger)
}
