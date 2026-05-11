package mqtt

import (
	"context"
	"encoding/json"
	"fmt"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Subscriber implements MQTTSubscriber using paho.mqtt.golang.
// It provides connection state tracking. Resubscription on reconnect
// is handled by wiring.go via the onConnected callback.
type Subscriber struct {
	config    config.MQTTConfig
	client    mqtt.Client
	connected bool
	logger    *logging.Logger
}

// NewSubscriber creates a new MQTT subscriber with an injected client.
func NewSubscriber(cfg config.MQTTConfig, client mqtt.Client, logger *logging.Logger) *Subscriber {
	return &Subscriber{
		config: cfg,
		client: client,
		logger: logger,
	}
}

// Subscribe connects to the broker and subscribes to the topic.
// The reconnect callback (resubscribe) is set up in wiring.go.
func (s *Subscriber) Subscribe(ctx context.Context, handler func(msg domain.RawTelemetryPayload) error) error {
	// Connect
	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt connect failed: %w", token.Error())
	}

	s.connected = true

	qos := byte(s.config.QOS)
	token := s.client.Subscribe(s.config.Topic, qos, func(_ mqtt.Client, msg mqtt.Message) {
		var payload domain.RawTelemetryPayload
		if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
			s.logger.Error("mqtt message parse error", "error", err)
			return
		}
		if err := handler(payload); err != nil {
			s.logger.Error("mqtt message handler error", "error", err)
		}
	})
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt subscribe failed: %w", token.Error())
	}

	s.logger.Info("mqtt subscribed", "topic", s.config.Topic, "qos", s.config.QOS)
	return nil
}

// IsConnected returns true if the client is connected.
func (s *Subscriber) IsConnected() bool {
	return s.connected && s.client != nil && s.client.IsConnected()
}

// Close disconnects from the broker.
func (s *Subscriber) Close() error {
	if s.client != nil {
		s.client.Disconnect(500)
	}
	s.logger.Info("mqtt disconnected")
	s.connected = false

	return nil
}