package httpadapter

import (
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
)

// TelemetryResponse represents the API response for telemetry data.
// Excludes raw_payload and idempotency_key from the response.
type TelemetryResponse struct {
	Time         time.Time   `json:"time"`
	ReceivedAt   time.Time   `json:"received_at"`
	DeviceID     string      `json:"device_id"`
	ProfileID    string      `json:"profile_id"`
	RegisterType string      `json:"register_type,omitempty"`
	Address      int         `json:"address,omitempty"`
	Count        int         `json:"count,omitempty"`
	Source       string      `json:"source,omitempty"`
	Metrics      interface{} `json:"metrics"`
}

// ChartResponse represents the API response for chart data.
// Uses domain.ChartSeries directly.
type ChartResponse struct {
	Series []domain.ChartSeries `json:"data"`
	Meta   interface{}          `json:"meta"`
}
