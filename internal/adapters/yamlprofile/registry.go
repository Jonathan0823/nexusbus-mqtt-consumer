package yamlprofile

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// Registry implements ProfileRegistry using YAML profiles.
type Registry struct {
	profiles map[string]domain.DeviceProfile
	logger   *logging.Logger
}

// NewRegistry creates a new profile registry from a YAML file.
func NewRegistry(path string, logger *logging.Logger) (*Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read profiles file: %w", err)
	}

	var file profilesFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parse profiles: %w", err)
	}

	profiles := make(map[string]domain.DeviceProfile)
	for _, p := range file.Profiles {
		profiles[p.ID] = p
	}

	logger.Info("profile registry loaded", "count", len(profiles))

	return &Registry{
		profiles: profiles,
		logger:   logger,
	}, nil
}

// profilesFile is the top-level structure of profiles.yaml.
type profilesFile struct {
	Profiles []domain.DeviceProfile `yaml:"profiles"`
}

// Match finds the best matching profile for a payload.
func (r *Registry) Match(payload domain.RawTelemetryPayload) (*domain.DeviceProfile, error) {
	// 1. Explicit profile_id from payload
	if payload.ProfileID != "" {
		if p, ok := r.profiles[payload.ProfileID]; ok {
			return &p, nil
		}
	}

	// 2. Exact device_id mapping
	for _, p := range r.profiles {
		if p.Match.DeviceIDExact != "" && p.Match.DeviceIDExact == payload.DeviceID {
			return &p, nil
		}
	}

	// 3. device_id prefix / regex match
	for _, p := range r.profiles {
		if p.Match.DeviceIDPrefix != "" && len(payload.DeviceID) >= len(p.Match.DeviceIDPrefix) {
			if payload.DeviceID[:len(p.Match.DeviceIDPrefix)] == p.Match.DeviceIDPrefix {
				return &p, nil
			}
		}
	}

	// 4. register_type + address + count match
	for _, p := range r.profiles {
		if p.Match.RegisterType != "" && p.Match.RegisterType != payload.RegisterType {
			continue
		}
		if p.Match.Address != 0 && p.Match.Address != payload.Address {
			continue
		}
		if p.Match.Count != 0 && p.Match.Count != payload.Count {
			continue
		}
		return &p, nil
	}

	// 5. Unknown profile fallback - return nil to indicate unknown
	return nil, nil
}

// Transform converts raw values to metrics using the matched profile.
func (r *Registry) Transform(payload domain.RawTelemetryPayload, profile *domain.DeviceProfile) (map[string]any, error) {
	// If no profile, use raw values
	if profile == nil {
		result := make(map[string]any)
		result["raw_values"] = payload.Values
		return result, nil
	}

	result := make(map[string]any)
	for _, mapping := range profile.Mapping {
		if mapping.Index >= len(payload.Values) {
			r.logger.Warn("mapping index out of range", "index", mapping.Index, "values_len", len(payload.Values))
			continue
		}

		rawValue := float64(payload.Values[mapping.Index])
		value := rawValue * mapping.Multiplier

		result[mapping.Key] = value
		if mapping.Unit != "" {
			result[mapping.Key+"_unit"] = mapping.Unit
		}
	}

	return result, nil
}
