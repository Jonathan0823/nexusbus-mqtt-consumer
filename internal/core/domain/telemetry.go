package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// HashString returns SHA256 hash of input string.
func HashString(input string) string {
	h := sha256.Sum256([]byte(input))
	return hex.EncodeToString(h[:])
}

// RawTelemetryPayload represents the incoming MQTT payload from Edge Gateway.
// Example: {"device_id":"office-eng","register_type":"input","address":0,"count":9,"values":[2239,643,0,1057,0,55290,4,499,73],"timestamp":1777357182.6396542}
type RawTelemetryPayload struct {
	DeviceID     string      `json:"device_id"`
	RegisterType string      `json:"register_type,omitempty"`
	Address      int         `json:"address,omitempty"`
	Count        int         `json:"count,omitempty"`
	Values       []int       `json:"values"`
	Timestamp    json.Number `json:"timestamp,omitempty"`
	MessageID    string      `json:"message_id,omitempty"`
	ProfileID    string      `json:"profile_id,omitempty"`
	Source       string      `json:"source,omitempty"`
}

// RawTelemetryMessage is the message as received and buffered in Redis Stream.
type RawTelemetryMessage struct {
	ID         string
	Payload    RawTelemetryPayload
	ReceivedAt time.Time
}

// BufferedMessage represents a message read from Redis Stream.
type BufferedMessage struct {
	ID      string
	Payload RawTelemetryPayload
}

// DeviceProfile defines how to map raw register values to semantic metrics.
type DeviceProfile struct {
	ID      string          `yaml:"id"`
	Match   ProfileMatch    `yaml:"match"`
	Mapping []MetricMapping `yaml:"mapping"`
}

// ProfileMatch defines which payloads match this profile.
type ProfileMatch struct {
	RegisterType   string `yaml:"register_type,omitempty"`
	Address        int    `yaml:"address,omitempty"`
	Count          int    `yaml:"count,omitempty"`
	DeviceIDPrefix string `yaml:"device_id_prefix,omitempty"`
	DeviceIDExact  string `yaml:"device_id_exact,omitempty"`
}

// MetricMapping maps a raw register index to a semantic metric.
type MetricMapping struct {
	Index      int     `yaml:"index"`
	Key        string  `yaml:"key"`
	Type       string  `yaml:"type"` // uint16, int16, etc.
	Multiplier float64 `yaml:"multiplier"`
	Unit       string  `yaml:"unit,omitempty"`
}

// EnrichedTelemetry is the transformed telemetry ready for database insertion.
type EnrichedTelemetry struct {
	Time           time.Time        `json:"time"`
	ReceivedAt     time.Time        `json:"received_at"`
	DeviceID       string           `json:"device_id"`
	ProfileID      string           `json:"profile_id"`
	RegisterType   string           `json:"register_type,omitempty"`
	Address        int              `json:"address,omitempty"`
	Count          int              `json:"count,omitempty"`
	Source         string           `json:"source,omitempty"`
	IdempotencyKey string           `json:"idempotency_key"`
	Metrics        map[string]any   `json:"metrics"`
	RawPayload     *json.RawMessage `json:"raw_payload,omitempty"`
}

// BuildIdempotencyKey creates a deterministic key for duplicate prevention.
// Priority: message_id (if present) → fallback to deterministic combination.
// The same payload must always produce the same key.
func BuildIdempotencyKey(payload RawTelemetryPayload, normalizedTime time.Time) string {
	// 1. Use message_id directly if provided
	if payload.MessageID != "" {
		return HashString(payload.MessageID)
	}

	// 2. Fallback: deterministic key from available fields
	// Prefer: device_id + timestamp_utc + profile_id
	// Otherwise: device_id + timestamp_utc + register_type + address + count
	timeStr := normalizedTime.UTC().Format(time.RFC3339Nano)

	if payload.ProfileID != "" {
		return HashString(fmt.Sprintf("%s|%s|%s", payload.DeviceID, timeStr, payload.ProfileID))
	}

	// Fallback: register_type + address + count
	return HashString(fmt.Sprintf("%s|%s|%s|%d|%d", payload.DeviceID, timeStr, payload.RegisterType, payload.Address, payload.Count))
}

// NormalizeTimestamp converts Unix epoch timestamp from payload to time.Time.
// Input: Unix epoch seconds with fractional part (e.g., 1777357182.6396542)
func NormalizeTimestamp(payload RawTelemetryPayload) (time.Time, error) {
	if payload.Timestamp == "" {
		return time.Now().UTC(), nil
	}

	// Parse as float64 first (epoch with fractional seconds)
	f, err := payload.Timestamp.Float64()
	if err != nil {
		return time.Time{}, err
	}

	sec := int64(f)
	nsec := int64((f - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC(), nil
}

// DeadletterMessage represents a message that failed permanently.
type DeadletterMessage struct {
	RedisStreamID   string              `json:"redis_stream_id"`
	FailedAt        time.Time           `json:"failed_at"`
	RetryCount      int                 `json:"retry_count"`
	Error           string              `json:"error"`
	OriginalPayload RawTelemetryPayload `json:"original_payload"`
}

// TelemetryRangeQuery represents a query for historical telemetry.
type TelemetryRangeQuery struct {
	DeviceID string
	From     time.Time
	To       time.Time
	Limit    int
}

// ValidationError represents a domain-level validation error.
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return "validation error: " + e.Field + " - " + e.Message
}

// IsValid checks if the raw payload has mandatory fields.
func (p RawTelemetryPayload) IsValid() error {
	if p.DeviceID == "" {
		return ValidationError{Field: "device_id", Message: "required"}
	}
	if len(p.Values) == 0 {
		return ValidationError{Field: "values", Message: "required and non-empty"}
	}
	return nil
}
