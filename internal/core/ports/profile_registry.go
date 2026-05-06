package ports

import (
	"modbus-mqtt-consumer/internal/core/domain"
)

// ProfileRegistry defines the interface for device profile matching and transformation.
type ProfileRegistry interface {
	// Match finds the best matching profile for a payload.
	Match(payload domain.RawTelemetryPayload) (*domain.DeviceProfile, error)
	// Transform converts raw values to metrics using the matched profile.
	Transform(payload domain.RawTelemetryPayload, profile *domain.DeviceProfile) (map[string]any, error)
}