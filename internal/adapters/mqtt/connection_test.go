package mqtt

import (
	"testing"

	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"
)

func TestNewClient_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	cfg := config.MQTTConfig{
		Broker:       "tcp://localhost:1883",
		ClientID:     "test-client",
		Topic:        "test/#",
		QOS:          1,
		CleanSession: false,
		Username:     "",
		Password:     "",
		Timeout:      0,
	}

	logger := logging.New("error")
	c, err := NewClient(cfg, logger, nil)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	if c == nil {
		t.Fatal("NewClient returned nil client")
	}
}