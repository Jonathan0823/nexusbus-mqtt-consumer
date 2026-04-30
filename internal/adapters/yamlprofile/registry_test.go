package yamlprofile

import (
	"math"
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

func TestMatchExplicitZeroAddress(t *testing.T) {
	t.Parallel()

	zero := 0
	one := 1
	r := &Registry{
		profiles: map[string]domain.DeviceProfile{
			"zero-address": {
				ID: "zero-address",
				Match: domain.ProfileMatch{
					RegisterType: "input",
					Address:      &zero,
					Count:        &one,
				},
			},
		},
		logger: logging.New("error"),
	}

	profile, err := r.Match(domain.RawTelemetryPayload{RegisterType: "input", Address: 0, Count: 1})
	if err != nil {
		t.Fatalf("match failed: %v", err)
	}
	if profile == nil || profile.ID != "zero-address" {
		t.Fatalf("expected zero-address profile, got %#v", profile)
	}
}

func TestMatchNonZeroAddressStillMatches(t *testing.T) {
	t.Parallel()

	four := 4
	six := 6
	r := &Registry{
		profiles: map[string]domain.DeviceProfile{
			"nonzero-address": {
				ID: "nonzero-address",
				Match: domain.ProfileMatch{
					RegisterType: "holding",
					Address:      &four,
					Count:        &six,
				},
			},
		},
		logger: logging.New("error"),
	}

	profile, err := r.Match(domain.RawTelemetryPayload{RegisterType: "holding", Address: 4, Count: 6})
	if err != nil {
		t.Fatalf("match failed: %v", err)
	}
	if profile == nil || profile.ID != "nonzero-address" {
		t.Fatalf("expected nonzero-address profile, got %#v", profile)
	}
}

func TestNewRegistryRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "profiles.yaml")
	yaml := []byte(`profiles:
  broken:
    match:
      device_id_prefix: "x-"
      unknown_field: true
    mapping: []
`)
	if err := os.WriteFile(path, yaml, 0o600); err != nil {
		t.Fatalf("write profiles: %v", err)
	}

	if _, err := NewRegistry(path, logging.New("error")); err == nil {
		t.Fatal("expected strict YAML parse error for unknown field")
	}
}

func TestTransformSupportsMetricMappingType(t *testing.T) {
	t.Parallel()

	r := &Registry{logger: logging.New("error")}
	profile := &domain.DeviceProfile{
		ID:      "typed",
		Mapping: []domain.MetricMapping{{Index: 0, Key: "signed", Type: "int16", Multiplier: 1}},
	}
	result, err := r.Transform(domain.RawTelemetryPayload{Values: []int{-1}}, profile)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}
	if result["signed"] != float64(-1) {
		t.Fatalf("expected signed metric -1, got %#v", result["signed"])
	}
}

func TestTransformRejectsUnsupportedMetricMappingType(t *testing.T) {
	t.Parallel()

	r := &Registry{logger: logging.New("error")}
	profile := &domain.DeviceProfile{
		ID:      "typed",
		Mapping: []domain.MetricMapping{{Index: 0, Key: "bad", Type: "bogus", Multiplier: 1}},
	}
	if _, err := r.Transform(domain.RawTelemetryPayload{Values: []int{1}}, profile); err == nil {
		t.Fatal("expected unsupported type error")
	}
}

func TestDecodeValue_Int16RangeValidation(t *testing.T) {
	t.Parallel()

	r := &Registry{logger: logging.New("error")}

	// Valid int16 min
	if _, err := r.decodeValue(math.MinInt16, "int16"); err != nil {
		t.Errorf("MinInt16 should be valid: %v", err)
	}
	// Valid int16 max
	if _, err := r.decodeValue(math.MaxInt16, "int16"); err != nil {
		t.Errorf("MaxInt16 should be valid: %v", err)
	}

	// Overflow below min
	if _, err := r.decodeValue(math.MinInt16-1, "int16"); err == nil {
		t.Error("expected error for value below int16 min")
	}
	// Overflow above max
	if _, err := r.decodeValue(math.MaxInt16+1, "int16"); err == nil {
		t.Error("expected error for value above int16 max")
	}
}

func TestDecodeValue_Int32RangeValidation(t *testing.T) {
	t.Parallel()

	r := &Registry{logger: logging.New("error")}

	// Valid int32 min
	if _, err := r.decodeValue(math.MinInt32, "int32"); err != nil {
		t.Errorf("MinInt32 should be valid: %v", err)
	}
	// Valid int32 max
	if _, err := r.decodeValue(math.MaxInt32, "int32"); err != nil {
		t.Errorf("MaxInt32 should be valid: %v", err)
	}

	// Overflow below min
	if _, err := r.decodeValue(math.MinInt32-1, "int32"); err == nil {
		t.Error("expected error for value below int32 min")
	}
	// Overflow above max
	if _, err := r.decodeValue(math.MaxInt32+1, "int32"); err == nil {
		t.Error("expected error for value above int32 max")
	}
}
