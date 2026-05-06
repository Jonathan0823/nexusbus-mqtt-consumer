package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
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
	Address        *int   `yaml:"address,omitempty"`
	Count          *int   `yaml:"count,omitempty"`
	DeviceIDPrefix string `yaml:"device_id_prefix,omitempty"`
	DeviceIDExact  string `yaml:"device_id_exact,omitempty"`
	Location       string `yaml:"location,omitempty"`
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
// Input: Unix epoch seconds with fractional part (e.g., 1777357182.6396542 or 1.7773571826396542e+09).
// When timestamp is empty, returns epoch zero (1970-01-01) to ensure deterministic
// idempotency keys across retries.
func NormalizeTimestamp(payload RawTelemetryPayload) (time.Time, error) {
	if payload.Timestamp == "" {
		return time.Unix(0, 0).UTC(), nil
	}

	raw := payload.Timestamp.String()

	// If the string contains exponent notation, parse with big.Rat to preserve precision.
	if strings.Contains(raw, "e") || strings.Contains(raw, "E") {
		// Split into mantissa and exponent: "1.777...e+09" -> mantissa="1.777...", exp=+9
		parts := strings.FieldsFunc(raw, func(r rune) bool {
			return r == 'e' || r == 'E'
		})
		if len(parts) != 2 {
			return time.Time{}, fmt.Errorf("invalid scientific notation: %s", raw)
		}
		mantissaStr := parts[0]
		expStr := parts[1]

		// Parse mantissa as a rational number (exact decimal representation).
		mantissa := new(big.Rat)
		if _, ok := mantissa.SetString(mantissaStr); !ok {
			return time.Time{}, fmt.Errorf("invalid mantissa: %s", mantissaStr)
		}

		// Parse exponent (may have + or - sign).
		exp, err := strconv.Atoi(expStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid exponent: %w", err)
		}

		// Multiply mantissa by 10^exp: value = mantissa * 10^exp.
		multiplier := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil))
		value := new(big.Rat).Mul(mantissa, multiplier)

		// Convert to total nanoseconds: value * 1e9.
		ns := new(big.Rat).Mul(value, big.NewRat(1_000_000_000, 1))
		totalNsec := ns.Num()
		// Ensure denominator is 1 (it should be after multiplication by integer 1e9).
		if ns.Denom().Sign() != 1 {
			totalNsec.Div(totalNsec, ns.Denom())
		}

		secBig := new(big.Int).Div(totalNsec, big.NewInt(1_000_000_000))
		nsecBig := new(big.Int).Rem(totalNsec, big.NewInt(1_000_000_000))

		sec := secBig.Int64()
		nsec := nsecBig.Int64()
		return time.Unix(sec, nsec).UTC(), nil
	}

	// Decimal path: exact string-based parsing to avoid floating-point rounding.
	secPart, fracPart, hasFrac := strings.Cut(raw, ".")
	sec, err := strconv.ParseInt(secPart, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	if !hasFrac {
		return time.Unix(sec, 0).UTC(), nil
	}

	if len(fracPart) > 9 {
		fracPart = fracPart[:9]
	}
	for len(fracPart) < 9 {
		fracPart += "0"
	}

	nsec, err := strconv.ParseInt(fracPart, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

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

// TelemetryQuery represents a query for dashboard telemetry.
type TelemetryQuery struct {
	DeviceID   string
	From       time.Time
	To         time.Time
	Limit      int
	MetricKeys []string
}

// Query constants for telemetry GET route.
const (
	MaxTimeRange   = 90 * 24 * time.Hour // 3 months in hours
	DefaultLimit   = 1000
	MaxLimit       = 50000
)

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
	if p.Address < 0 {
		return ValidationError{Field: "address", Message: "must be non-negative"}
	}
	if p.Count < 0 {
		return ValidationError{Field: "count", Message: "must be non-negative"}
	}
	if p.Count > 0 && p.Count != len(p.Values) {
		return ValidationError{Field: "count", Message: "must match values length when provided"}
	}
	return nil
}
