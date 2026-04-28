package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// Subscriber implements MQTTSubscriber using paho.mqtt.golang.
type Subscriber struct {
	config    MQTTConfig
	client    mqtt.Client
	connected bool
	mu        sync.RWMutex
	onMessage func(payload domain.RawTelemetryPayload) error
	logger    *logging.Logger
}

// MQTTConfig holds MQTT connection settings (local subset).
type MQTTConfig struct {
	Broker        string
	ClientID      string
	Topic         string
	QOS           byte
	CleanSession  bool
	Username      string
	Password      string
	SessionExpiry time.Duration
	Timeout       time.Duration
}

// NewSubscriber creates a new MQTT subscriber.
func NewSubscriber(cfg MQTTConfig, logger *logging.Logger) *Subscriber {
	return &Subscriber{
		config: cfg,
		logger: logger,
	}
}

// Subscribe connects to the broker and starts consuming messages.
func (s *Subscriber) Subscribe(ctx context.Context, handler func(msg domain.RawTelemetryPayload) error) error {
	s.onMessage = handler

	opts := mqtt.NewClientOptions().
		SetClientID(s.config.ClientID).
		SetCleanSession(s.config.CleanSession).
		AddBroker(s.config.Broker).
		SetConnectTimeout(s.config.Timeout).
		SetOnConnectHandler(s.onConnect).
		SetConnectionLostHandler(s.onConnectionLost).
		SetDefaultPublishHandler(s.onPublish)

	if s.config.Username != "" {
		opts.SetUsername(s.config.Username)
		opts.SetPassword(s.config.Password)
	}

	s.client = mqtt.NewClient(opts)

	// Connect with context
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt connect failed: %w", token.Error())
	}

	<-connectCtx.Done()

	// Subscribe to topic
	topicHandler := func(client mqtt.Client, msg mqtt.Message) {
		s.handleMessage(msg)
	}

	token := s.client.Subscribe(s.config.Topic, s.config.QOS, topicHandler)
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

	// Call the registered handler
	if s.onMessage != nil {
		if err := s.onMessage(payload); err != nil {
			s.logger.Error("mqtt message handler error", "error", err)
		}
	}
}

// onConnect is called when the client connects.
func (s *Subscriber) onConnect(client mqtt.Client) {
	s.mu.Lock()
	s.connected = true
	s.mu.Unlock()
	s.logger.Info("mqtt connected")
}

// onConnectionLost is called when the connection is lost.
func (s *Subscriber) onConnectionLost(client mqtt.Client, err error) {
	s.mu.Lock()
	s.connected = false
	s.mu.Unlock()
	s.logger.Error("mqtt connection lost", "error", err)
}

// onPublish is the default publish handler.
func (s *Subscriber) onPublish(client mqtt.Client, msg mqtt.Message) {
	s.logger.Debug("mqtt message received", "topic", msg.Topic())
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
	return nil
}
