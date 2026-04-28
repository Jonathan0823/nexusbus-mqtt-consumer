package yamlprofile

import (
	"os"
	"path/filepath"
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

func TestNewRegistryParsesMapProfiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "profiles.yaml")
	yaml := []byte(`profiles:
  power_meter_9:
    match:
      device_id_prefix: "office-"
    mapping: []
  power_meter_3:
    match:
      device_id_prefix: "lab-"
    mapping: []
`)
	if err := os.WriteFile(path, yaml, 0o600); err != nil {
		t.Fatalf("write profiles: %v", err)
	}

	r, err := NewRegistry(path, logging.New("error"))
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	profile, err := r.Match(domain.RawTelemetryPayload{DeviceID: "office-a"})
	if err != nil {
		t.Fatalf("match failed: %v", err)
	}
	if profile == nil || profile.ID != "power_meter_9" {
		t.Fatalf("expected power_meter_9, got %#v", profile)
	}

	profile, err = r.Match(domain.RawTelemetryPayload{DeviceID: "lab-a"})
	if err != nil {
		t.Fatalf("match failed: %v", err)
	}
	if profile == nil || profile.ID != "power_meter_3" {
		t.Fatalf("expected power_meter_3, got %#v", profile)
	}
}
