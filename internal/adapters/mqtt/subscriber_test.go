package mqtt

import (
	"testing"
	"time"

	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
)

// mockClient is a test double for mqtt.Client.
type mockClient struct {
	connected bool
}

func (c *mockClient) IsConnected() bool       { return c.connected }
func (c *mockClient) IsConnectionOpen() bool  { return c.connected }
func (c *mockClient) Connect() mqtt.Token     { return &token{} }
func (c *mockClient) Disconnect(quiesce uint) {}
func (c *mockClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	return &token{}
}
func (c *mockClient) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	return &token{}
}
func (c *mockClient) SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token {
	return &token{}
}
func (c *mockClient) Unsubscribe(topics ...string) mqtt.Token             { return &token{} }
func (c *mockClient) AddRoute(topic string, callback mqtt.MessageHandler) {}
func (c *mockClient) OptionsReader() mqtt.ClientOptionsReader             { return mqtt.ClientOptionsReader{} }

// token implements mqtt.Token using the private tokenCompletor.
type token struct{ err error }

func (t *token) Wait() bool                     { return true }
func (t *token) WaitTimeout(time.Duration) bool { return true }
func (t *token) Error() error                   { return t.err }
func (t *token) Done() <-chan struct{}          { return nil }

func TestSubscriberIsReady(t *testing.T) {
	t.Parallel()

	logger := logging.New("error")
	cfg := config.MQTTConfig{
		Broker:   "tcp://localhost:1883",
		Topic:    "test/topic",
		ClientID: "test-subscriber",
		QOS:      0,
	}

	// Test: mock transport connected, Subscriber not yet ready (not subscribed)
	s := NewSubscriber(cfg, &mockClient{connected: true}, logger)
	s.SetConnected(true) // wiring.go calls this after reconnect
	assert.False(t, s.IsReady(), "should not be ready when subscribed=false")
	assert.True(t, s.IsConnected(), "transport should be connected")

	// Test: connected transport, subscribed => ready
	s.SetSubscribed(true)
	assert.True(t, s.IsReady(), "should be ready when connected && subscribed")

	// Test: subscription state doesn't affect IsConnected (transport only)
	s.SetSubscribed(false)
	assert.True(t, s.IsConnected(), "IsConnected reflects transport only")
}

func TestSubscriberSetSubscribedResetsState(t *testing.T) {
	t.Parallel()

	logger := logging.New("error")
	client := &mockClient{connected: true}
	s := NewSubscriber(mqttConfig(), client, logger)
	s.SetConnected(true)
	s.SetSubscribed(true)

	// Transport drops (mock underlying client)
	client.connected = false
	assert.False(t, s.IsReady(), "not ready when transport dropped even if subscribed=true")
}

func TestSubscriberReadyRequiresBothStates(t *testing.T) {
	t.Parallel()

	logger := logging.New("error")
	client := &mockClient{connected: true}
	s := NewSubscriber(mqttConfig(), client, logger)

	// Neither connected nor subscribed
	assert.False(t, s.IsReady())
	assert.False(t, s.IsConnected())

	// Only connected
	s.SetConnected(true)
	assert.False(t, s.IsReady(), "not ready without subscription")

	// Both connected and subscribed
	s.SetSubscribed(true)
	assert.True(t, s.IsReady())

	// Only subscribed (transport down)
	client.connected = false
	assert.False(t, s.IsReady(), "not ready when transport down")
}

func mqttConfig() config.MQTTConfig {
	return config.MQTTConfig{
		Broker:   "tcp://localhost:1883",
		Topic:    "test/topic",
		ClientID: "test-subscriber",
		QOS:      0,
	}
}
