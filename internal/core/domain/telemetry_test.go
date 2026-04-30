package domain

import (
	"testing"
	"time"
)

func TestNormalizeTimestamp_EmptyReturnsEpochZero(t *testing.T) {
	payload := RawTelemetryPayload{
		DeviceID: "test-device",
		Values:   []int{1, 2, 3},
		// Timestamp is empty
	}

	result, err := NormalizeTimestamp(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return epoch zero (1970-01-01 00:00:00 UTC)
	expected := time.Unix(0, 0).UTC()
	if !result.Equal(expected) {
		t.Errorf("expected epoch zero, got %v", result)
	}
}

func TestNormalizeTimestamp_ValidTimestamp(t *testing.T) {
	payload := RawTelemetryPayload{
		DeviceID:  "test-device",
		Values:    []int{1, 2, 3},
		Timestamp: "1777357182.6396542",
	}

	result, err := NormalizeTimestamp(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the timestamp is correctly parsed
	expectedSec := int64(1777357182)
	expectedNsec := 639654200 // fractional part * 1e9

	if result.Unix() != expectedSec {
		t.Errorf("expected seconds %d, got %d", expectedSec, result.Unix())
	}
	if result.Nanosecond() != expectedNsec {
		t.Errorf("expected nanoseconds %d, got %d", expectedNsec, result.Nanosecond())
	}
}

func TestNormalizeTimestamp_InvalidTimestamp(t *testing.T) {
	payload := RawTelemetryPayload{
		DeviceID:  "test-device",
		Values:    []int{1, 2, 3},
		Timestamp: "not-a-number",
	}

	_, err := NormalizeTimestamp(payload)
	if err == nil {
		t.Error("expected error for invalid timestamp, got nil")
	}
}

func TestNormalizeTimestamp_ScientificNotation(t *testing.T) {
	// Example: 1.7773571826396542e+09 represents 1777357182.6396542
	payload := RawTelemetryPayload{
		DeviceID:  "test-device",
		Values:    []int{1, 2, 3},
		Timestamp: "1.7773571826396542e+09",
	}

	result, err := NormalizeTimestamp(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedSec := int64(1777357182)
	expectedNsec := 639654200

	if result.Unix() != expectedSec {
		t.Errorf("expected seconds %d, got %d", expectedSec, result.Unix())
	}
	if result.Nanosecond() != expectedNsec {
		t.Errorf("expected nanoseconds %d, got %d", expectedNsec, result.Nanosecond())
	}
}

func TestBuildIdempotencyKey_NoTimestampNoMessageID_Deterministic(t *testing.T) {
	// Create payload without timestamp and message_id
	payload := RawTelemetryPayload{
		DeviceID:     "office-eng",
		RegisterType: "input",
		Address:      0,
		Count:        9,
		Values:       []int{2239, 643, 0, 1057, 0, 55290, 4, 499, 73},
	}

	// Get normalized time (will be epoch zero)
	normalizedTime, err := NormalizeTimestamp(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Call BuildIdempotencyKey multiple times - should produce same result
	key1 := BuildIdempotencyKey(payload, normalizedTime)
	key2 := BuildIdempotencyKey(payload, normalizedTime)
	key3 := BuildIdempotencyKey(payload, normalizedTime)

	if key1 != key2 {
		t.Errorf("key1 (%s) != key2 (%s)", key1, key2)
	}
	if key2 != key3 {
		t.Errorf("key2 (%s) != key3 (%s)", key2, key3)
	}

	// Verify it's a valid SHA256 hash (64 hex characters)
	if len(key1) != 64 {
		t.Errorf("expected hash length 64, got %d", len(key1))
	}
}

func TestBuildIdempotencyKey_WithMessageID(t *testing.T) {
	payload := RawTelemetryPayload{
		DeviceID:  "test-device",
		Values:    []int{1, 2, 3},
		MessageID: "msg-12345",
	}

	normalizedTime := time.Now() // time doesn't matter when message_id is present

	key := BuildIdempotencyKey(payload, normalizedTime)

	// Should be hash of message_id
	expected := HashString("msg-12345")
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}

func TestBuildIdempotencyKey_WithTimestamp(t *testing.T) {
	payload := RawTelemetryPayload{
		DeviceID:     "test-device",
		RegisterType: "holding",
		Address:      10,
		Count:        5,
		Values:       []int{100, 200, 300, 400, 500},
		Timestamp:    "1777357182.6396542",
	}

	normalizedTime, err := NormalizeTimestamp(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	key := BuildIdempotencyKey(payload, normalizedTime)

	// Verify key is deterministic - same inputs produce same output
	key2 := BuildIdempotencyKey(payload, normalizedTime)
	if key != key2 {
		t.Errorf("key not deterministic: %s != %s", key, key2)
	}

	// Verify it's a valid SHA256 hash
	if len(key) != 64 {
		t.Errorf("expected hash length 64, got %d", len(key))
	}
}

func TestBuildIdempotencyKey_WithProfileID(t *testing.T) {
	payload := RawTelemetryPayload{
		DeviceID:  "test-device",
		Values:    []int{1, 2, 3},
		ProfileID: "profile-abc",
		Timestamp: "1777357182.6396542",
	}

	normalizedTime, err := NormalizeTimestamp(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	key := BuildIdempotencyKey(payload, normalizedTime)

	// Should include profile_id and the normalized timestamp in the hash
	expected := HashString("test-device|" + normalizedTime.UTC().Format(time.RFC3339Nano) + "|profile-abc")
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}

func TestBuildIdempotencyKey_DifferentPayloadsDifferentKeys(t *testing.T) {
	payload1 := RawTelemetryPayload{
		DeviceID:     "device-1",
		RegisterType: "input",
		Address:      0,
		Count:        5,
		Values:       []int{1, 2, 3, 4, 5},
	}

	payload2 := RawTelemetryPayload{
		DeviceID:     "device-2",
		RegisterType: "input",
		Address:      0,
		Count:        5,
		Values:       []int{1, 2, 3, 4, 5},
	}

	normalizedTime := time.Unix(0, 0).UTC()

	key1 := BuildIdempotencyKey(payload1, normalizedTime)
	key2 := BuildIdempotencyKey(payload2, normalizedTime)

	if key1 == key2 {
		t.Error("different payloads should produce different idempotency keys")
	}
}

func TestHashString_OutputFormat(t *testing.T) {
	result := HashString("test-input")

	// SHA256 produces 64 hex characters
	if len(result) != 64 {
		t.Errorf("expected 64 characters, got %d", len(result))
	}

	// Should be all lowercase hex
	for _, c := range result {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("invalid hex character: %c", c)
		}
	}
}
