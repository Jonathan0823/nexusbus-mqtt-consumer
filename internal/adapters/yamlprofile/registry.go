package yamlprofile

import (
	"fmt"
	"os"
	"strings"

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
	if payload.ProfileID != "" {
		if profile, ok := r.profiles[payload.ProfileID]; ok {
			return cloneProfile(profile), nil
		}
	}

	if profile := r.matchByExactDeviceID(payload.DeviceID); profile != nil {
		return cloneProfile(*profile), nil
	}

	if profile := r.matchByDevicePrefix(payload.DeviceID); profile != nil {
		return cloneProfile(*profile), nil
	}

	if profile := r.matchByRegisters(payload); profile != nil {
		return cloneProfile(*profile), nil
	}

	return nil, nil
}

func (r *Registry) matchByExactDeviceID(deviceID string) *domain.DeviceProfile {
	for _, profile := range r.profiles {
		if profile.Match.DeviceIDExact == deviceID && deviceID != "" {
			p := profile
			return &p
		}
	}

	return nil
}

func (r *Registry) matchByDevicePrefix(deviceID string) *domain.DeviceProfile {
	for _, profile := range r.profiles {
		if profile.Match.DeviceIDPrefix == "" || deviceID == "" {
			continue
		}
		if strings.HasPrefix(deviceID, profile.Match.DeviceIDPrefix) {
			p := profile
			return &p
		}
	}

	return nil
}

func (r *Registry) matchByRegisters(payload domain.RawTelemetryPayload) *domain.DeviceProfile {
	for _, profile := range r.profiles {
		if profile.Match.RegisterType != "" && profile.Match.RegisterType != payload.RegisterType {
			continue
		}
		if profile.Match.Address != 0 && profile.Match.Address != payload.Address {
			continue
		}
		if profile.Match.Count != 0 && profile.Match.Count != payload.Count {
			continue
		}
		p := profile
		return &p
	}

	return nil
}

func cloneProfile(profile domain.DeviceProfile) *domain.DeviceProfile {
	p := profile
	return &p
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
