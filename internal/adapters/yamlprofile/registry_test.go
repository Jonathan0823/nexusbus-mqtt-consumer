package yamlprofile

import (
	"testing"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

func TestMatchReturnsStableProfileCopy(t *testing.T) {
	t.Parallel()

	r := &Registry{
		profiles: map[string]domain.DeviceProfile{
			"power_meter_9": {
				ID:    "power_meter_9",
				Match: domain.ProfileMatch{DeviceIDPrefix: "office-"},
			},
		},
		logger: logging.New("error"),
	}

	first, err := r.Match(domain.RawTelemetryPayload{DeviceID: "office-a", ProfileID: "power_meter_9"})
	if err != nil {
		t.Fatalf("match failed: %v", err)
	}
	second, err := r.Match(domain.RawTelemetryPayload{DeviceID: "office-b", ProfileID: "power_meter_9"})
	if err != nil {
		t.Fatalf("match failed: %v", err)
	}

	if first == second {
		t.Fatal("expected distinct profile pointers")
	}
	if first == nil || second == nil {
		t.Fatal("expected non-nil profiles")
	}
	if first.ID != "power_meter_9" || second.ID != "power_meter_9" {
		t.Fatal("unexpected profile IDs")
	}
}

func TestMatchReturnsNilForUnknownProfile(t *testing.T) {
	t.Parallel()

	r := &Registry{profiles: map[string]domain.DeviceProfile{}, logger: logging.New("error")}
	profile, err := r.Match(domain.RawTelemetryPayload{DeviceID: ""})
	if err != nil {
		t.Fatalf("match failed: %v", err)
	}
	if profile != nil {
		t.Fatal("expected nil profile for unknown payload")
	}
}
