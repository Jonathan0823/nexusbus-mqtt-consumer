package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Subscriber implements MQTTSubscriber using paho.mqtt.golang.
// It tracks both transport connection and subscription state for health checks.
type Subscriber struct {
	config     config.MQTTConfig
	client     mqtt.Client
	connected  atomic.Bool
	subscribed atomic.Bool
	logger     *logging.Logger
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

	s.connected.Store(true)

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

	s.subscribed.Store(true)
	s.logger.Info("mqtt subscribed", "topic", s.config.Topic, "qos", s.config.QOS)
	return nil
}

// SetConnected sets the transport connection state.
func (s *Subscriber) SetConnected(v bool) {
	s.connected.Store(v)
}

// SetSubscribed marks the subscription state. Call this from reconnect callbacks
// to update readiness after resubscription attempts.
func (s *Subscriber) SetSubscribed(v bool) {
	s.subscribed.Store(v)
}

// IsReady returns true if the client is connected and subscribed to the topic.
func (s *Subscriber) IsReady() bool {
	return s.connected.Load() && s.subscribed.Load() && s.client != nil && s.client.IsConnected()
}

// IsConnected returns true if the client transport is connected.
// Deprecated: use IsReady for health checks.
func (s *Subscriber) IsConnected() bool {
	return s.connected.Load() && s.client != nil && s.client.IsConnected()
}

// Close disconnects from the broker.
func (s *Subscriber) Close() error {
	if s.client != nil {
		s.client.Disconnect(500)
	}
	s.logger.Info("mqtt disconnected")
	s.connected.Store(false)
	s.subscribed.Store(false)

	return nil
}
