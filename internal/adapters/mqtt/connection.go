package mqtt

import "modbus-mqtt-consumer/internal/platform/logging"

// NewConnection creates an MQTT subscriber.
// This factory follows the design.md adapter creation pattern.
func NewConnection(cfg MQTTConfig, logger *logging.Logger) *Subscriber {
	return NewSubscriber(cfg, logger)
}
