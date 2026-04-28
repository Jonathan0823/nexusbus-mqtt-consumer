package domain

import (
	"testing"
	"time"
)

func TestBuildIdempotencyKeyPrefersMessageID(t *testing.T) {
	t.Parallel()

	payload := RawTelemetryPayload{
		DeviceID:     "office-eng",
		MessageID:    "msg-123",
		ProfileID:    "power_meter_9",
		RegisterType: "input",
		Address:      0,
		Count:        9,
	}

	got := BuildIdempotencyKey(payload, time.Unix(100, 0))
	want := HashString("msg-123")

	if got != want {
		t.Fatalf("expected message_id hash %q, got %q", want, got)
	}
}

func TestBuildIdempotencyKeyIsDeterministic(t *testing.T) {
	t.Parallel()

	payload := RawTelemetryPayload{
		DeviceID:     "office-eng",
		ProfileID:    "power_meter_9",
		RegisterType: "input",
		Address:      0,
		Count:        9,
	}

	first := BuildIdempotencyKey(payload, time.Unix(100, 0).UTC())
	second := BuildIdempotencyKey(payload, time.Unix(100, 0).UTC())

	if first != second {
		t.Fatalf("expected deterministic key, got %q and %q", first, second)
	}
}
