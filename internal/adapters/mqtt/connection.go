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
