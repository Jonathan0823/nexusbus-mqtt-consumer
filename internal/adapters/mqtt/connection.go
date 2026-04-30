package mqtt

import (
	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// NewClient creates a new MQTT client (not yet connected).
func NewClient(cfg config.MQTTConfig, logger *logging.Logger) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		SetClientID(cfg.ClientID).
		SetCleanSession(cfg.CleanSession).
		AddBroker(cfg.Broker).
		SetConnectTimeout(cfg.Timeout)

	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}

	return mqtt.NewClient(opts), nil
}

// NewConnection is a compatibility shim that creates client and subscriber.
// Deprecated: use NewClient + NewSubscriber instead.
func NewConnection(cfg config.MQTTConfig, logger *logging.Logger) *Subscriber {
	client, err := NewClient(cfg, logger)
	if err != nil {
		// In practice this shouldn't happen during construction,
		// but we panic here because NewSubscriber requires a client.
		panic(err)
	}
	return NewSubscriber(cfg, client, logger)
}
