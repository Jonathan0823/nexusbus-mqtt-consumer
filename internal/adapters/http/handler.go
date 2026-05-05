package httpadapter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/ports"
	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/metrics"
)

// Handler holds HTTP handlers and dependencies.
type Handler struct {
	logger           *logging.Logger
	metrics          *metrics.Recorder
	telemetryService ports.TelemetryService
	mqttPing         func() error
	redisPing        func() error
	postgresPing     func() error
	profilesLoaded   bool
}

// NewHandler creates a new HTTP handler.
func NewHandler(
	logger *logging.Logger,
	metrics *metrics.Recorder,
	telemetryService ports.TelemetryService,
	mqttPing func() error,
	redisPing func() error,
	postgresPing func() error,
	profilesLoaded bool,
) *Handler {
	return &Handler{
		logger:           logger,
		metrics:          metrics,
		telemetryService: telemetryService,
		mqttPing:         mqttPing,
		redisPing:        redisPing,
		postgresPing:     postgresPing,
		profilesLoaded:   profilesLoaded,
	}
}

// Healthz returns OK if the service is running.
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

// Readyz returns OK only if critical dependencies are reachable.
func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	checks := make(map[string]string)
	allHealthy := true

	// Check Redis
	if h.mqttPing != nil {
		if err := h.mqttPing(); err != nil {
			checks["mqtt"] = "unhealthy"
			allHealthy = false
		} else {
			checks["mqtt"] = "healthy"
		}
	}

	// Check Redis
	if h.redisPing != nil {
		if err := h.redisPing(); err != nil {
			checks["redis"] = "unhealthy"
			allHealthy = false
		} else {
			checks["redis"] = "healthy"
		}
	}

	// Check PostgreSQL
	if h.postgresPing != nil {
		if err := h.postgresPing(); err != nil {
			checks["postgres"] = "unhealthy"
			allHealthy = false
		} else {
			checks["postgres"] = "healthy"
		}
	}

	// Check profiles
	if !h.profilesLoaded {
		checks["profiles"] = "not loaded"
		allHealthy = false
	} else {
		checks["profiles"] = "loaded"
	}

	status := "ok"
	statusCode := http.StatusOK
	if !allHealthy {
		status = "not ready"
		statusCode = http.StatusServiceUnavailable
	}

	response := map[string]interface{}{
		"status": status,
		"checks": checks,
	}

	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}

// Metrics returns Prometheus-formatted metrics.
func (h *Handler) Metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, h.metrics.String())
}

// TelemetryResponse represents the API response for telemetry data.
// Excludes raw_payload and idempotency_key from the response.
type TelemetryResponse struct {
	Time          time.Time   `json:"time"`
	ReceivedAt    time.Time   `json:"received_at"`
	DeviceID      string      `json:"device_id"`
	ProfileID     string      `json:"profile_id"`
	RegisterType  string      `json:"register_type,omitempty"`
	Address       int         `json:"address,omitempty"`
	Count         int         `json:"count,omitempty"`
	Source        string      `json:"source,omitempty"`
	Metrics       interface{} `json:"metrics"`
}

// GetTelemetry returns telemetry data for a device.
func (h *Handler) GetTelemetry(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Extract device_id from path - format is /api/v1/devices/{device_id}/telemetry
	deviceID := extractDeviceID(r.URL.Path)
	if deviceID == "" {
		h.sendError(w, http.StatusBadRequest, "device_id is required")
		return
	}

	// Parse query parameters
	query := r.URL.Query()

	// Parse 'from' - default to 24 hours ago
	fromStr := query.Get("from")
	var from time.Time
	if fromStr != "" {
		var err error
		from, err = time.Parse(time.RFC3339, fromStr)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, "invalid 'from' timestamp format")
			return
		}
	} else {
		from = time.Now().UTC().Add(-24 * time.Hour)
	}

	// Parse 'to' - default to now
	toStr := query.Get("to")
	var to time.Time
	if toStr != "" {
		var err error
		to, err = time.Parse(time.RFC3339, toStr)
		if err != nil {
			h.sendError(w, http.StatusBadRequest, "invalid 'to' timestamp format")
			return
		}
	} else {
		to = time.Now().UTC()
	}

	// Validate time range - max 3 months
	// Validate that 'from' precedes 'to'
	if !from.Before(to) {
		h.sendError(w, http.StatusBadRequest, "'from' must be before 'to'")
		return
	}
	if to.Sub(from) > domain.MaxTimeRange {
		h.sendError(w, http.StatusBadRequest, "time range exceeds maximum of 3 months")
		return
	}

	// Parse limit - default 1000, max 50000
	limit := domain.DefaultLimit
	if limitStr := query.Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			h.sendError(w, http.StatusBadRequest, "invalid limit value")
			return
		}
		if limit > domain.MaxLimit {
			h.sendError(w, http.StatusBadRequest, fmt.Sprintf("limit exceeds maximum of %d", domain.MaxLimit))
			return
		}
	}

	// Parse metrics - optional comma-separated list
	var metricKeys []string
	if metricsStr := query.Get("metrics"); metricsStr != "" {
		metricKeys = strings.Split(metricsStr, ",")
		for i, key := range metricKeys {
			metricKeys[i] = strings.TrimSpace(key)
		}
	}

	// Build query
	telemetryQuery := domain.TelemetryQuery{
		DeviceID:   deviceID,
		From:       from,
		To:         to,
		Limit:      limit,
		MetricKeys: metricKeys,
	}

	// Call service
	results, err := h.telemetryService.QueryDeviceTelemetry(r.Context(), telemetryQuery)
	if err != nil {
		h.logger.Error("query telemetry failed", "error", err)
		h.sendError(w, http.StatusInternalServerError, "failed to query telemetry")
		return
	}

	// Build response - exclude raw_payload and idempotency_key
	responseData := make([]TelemetryResponse, 0, len(results))
	for _, r := range results {
		responseData = append(responseData, TelemetryResponse{
			Time:         r.Time,
			ReceivedAt:   r.ReceivedAt,
			DeviceID:     r.DeviceID,
			ProfileID:    r.ProfileID,
			RegisterType: r.RegisterType,
			Address:      r.Address,
			Count:        r.Count,
			Source:       r.Source,
			Metrics:      r.Metrics,
		})
	}

	response := map[string]interface{}{
		"data": responseData,
		"meta": map[string]interface{}{
			"device_id": deviceID,
			"from":      from.Format(time.RFC3339),
			"to":        to.Format(time.RFC3339),
			"limit":     limit,
			"count":     len(results),
		},
	}

	if len(metricKeys) > 0 {
		response["meta"].(map[string]interface{})["metrics"] = metricKeys
	}

	json.NewEncoder(w).Encode(response)
}

// extractDeviceID extracts device_id from URL path.
// Path format: /api/v1/devices/{device_id}/telemetry
func extractDeviceID(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == "devices" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// sendError sends a JSON error response.
func (h *Handler) sendError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}


