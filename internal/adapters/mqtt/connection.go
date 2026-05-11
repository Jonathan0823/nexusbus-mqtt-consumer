package mqtt

import (
	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// NewClient creates a new MQTT client with auto-reconnect.
// onConnected is called (with the subscribed client) after every successful
// connect and reconnect. The caller should perform the first subscribe inside it.
func NewClient(cfg config.MQTTConfig, logger *logging.Logger, onConnected func(mqtt.Client)) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		SetClientID(cfg.ClientID).
		SetCleanSession(cfg.CleanSession).
		AddBroker(cfg.Broker).
		SetConnectTimeout(cfg.Timeout).
		SetAutoReconnect(true).
		SetConnectRetryInterval(5).
		SetOrderMatters(false)

	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}

	l := logger.With("mqtt.client_id", cfg.ClientID, "mqtt.broker", cfg.Broker)

	// Fired on every successful connect (first and reconnects).
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		l.Info("mqtt connected")
		if onConnected != nil {
			onConnected(client)
		}
	})

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		l.Warn("mqtt connection lost", "error", err)
	})

	opts.SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
		l.Info("mqtt reconnecting")
	})

	client := mqtt.NewClient(opts)
	if client == nil {
		return nil, nil
	}

	return client, nil
}