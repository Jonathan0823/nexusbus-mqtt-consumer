package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Subscriber implements MQTTSubscriber using paho.mqtt.golang.
type Subscriber struct {
	config    config.MQTTConfig
	client    mqtt.Client
	connected bool
	mu        sync.RWMutex
	onMessage func(payload domain.RawTelemetryPayload) error
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

// Subscribe connects to the broker and starts consuming messages.
func (s *Subscriber) Subscribe(ctx context.Context, handler func(msg domain.RawTelemetryPayload) error) error {
	s.onMessage = handler

	// Connect
	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt connect failed: %w", token.Error())
	}

	s.mu.Lock()
	s.connected = true
	s.mu.Unlock()

	// Subscribe to topic
	topicHandler := func(_ mqtt.Client, msg mqtt.Message) {
		s.handleMessage(msg)
	}

	qos := byte(s.config.QOS)
	token := s.client.Subscribe(s.config.Topic, qos, topicHandler)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt subscribe failed: %w", token.Error())
	}

	s.logger.Info("mqtt subscribed", "topic", s.config.Topic, "qos", s.config.QOS)
	return nil
}

// handleMessage processes an incoming MQTT message.
func (s *Subscriber) handleMessage(msg mqtt.Message) {
	var payload domain.RawTelemetryPayload
	if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
		s.logger.Error("mqtt message parse error", "error", err)
		return
	}

	if s.onMessage != nil {
		if err := s.onMessage(payload); err != nil {
			s.logger.Error("mqtt message handler error", "error", err)
		}
	}
}

// IsConnected returns true if the client is connected.
func (s *Subscriber) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected && s.client != nil && s.client.IsConnected()
}

// Close disconnects from the broker.
func (s *Subscriber) Close() error {
	if s.client != nil {
		s.client.Disconnect(500)
	}
	s.logger.Info("mqtt disconnected")

	s.mu.Lock()
	s.connected = false
	s.mu.Unlock()

	return nil
}
