package httpadapter

import (
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/ports"
	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/metrics"
)

// Handler holds HTTP handlers and dependencies.
type Handler struct {
	logger           *logging.Logger
	metrics         *metrics.Recorder
	telemetryService ports.TelemetryService
	mqttPing        func() error
	redisPing       func() error
	postgresPing    func() error
	profilesLoaded bool
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
		postgresPing:    postgresPing,
		profilesLoaded:  profilesLoaded,
	}
}

// Healthz returns OK if the service is running.
func (h *Handler) Healthz(c *gin.Context) {
	c.JSON(200, map[string]string{
		"status": "ok",
	})
}

// Readyz returns OK only if critical dependencies are reachable.
func (h *Handler) Readyz(c *gin.Context) {
	checks := make(map[string]string)
	allHealthy := true

	// Check MQTT
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
	statusCode := 200
	if !allHealthy {
		status = "not ready"
		statusCode = 503
	}

	response := map[string]interface{}{
		"status": status,
		"checks": checks,
	}

	c.JSON(statusCode, response)
}

// Metrics returns Prometheus-formatted metrics.
func (h *Handler) Metrics(c *gin.Context) {
	c.Header("Content-Type", "text/plain; version=0.0.4")
	c.String(200, h.metrics.String())
}
// GetTelemetry returns telemetry data for a device.
func (h *Handler) GetTelemetry(c *gin.Context) {
	// Extract device_id from path parameter
	deviceID := c.Param("device_id")
	if deviceID == "" {
		h.sendError(c, 400, "device_id is required")
		return
	}

	// Parse query parameters
	// Parse 'from' - default to 24 hours ago
	fromStr := c.Query("from")
	var from time.Time
	if fromStr != "" {
		var err error
		from, err = time.Parse(time.RFC3339, fromStr)
		if err != nil {
			h.sendError(c, 400, "invalid 'from' timestamp format")
			return
		}
	} else {
		from = time.Now().UTC().Add(-24 * time.Hour)
	}

	// Parse 'to' - default to now
	toStr := c.Query("to")
	var to time.Time
	if toStr != "" {
		var err error
		to, err = time.Parse(time.RFC3339, toStr)
		if err != nil {
			h.sendError(c, 400, "invalid 'to' timestamp format")
			return
		}
	} else {
		to = time.Now().UTC()
	}

	// Validate time range - max 3 months
	// Validate that 'from' precedes 'to'
	if !from.Before(to) {
		h.sendError(c, 400, "'from' must be before 'to'")
		return
	}
	if to.Sub(from) > domain.MaxTimeRange {
		h.sendError(c, 400, "time range exceeds maximum of 3 months")
		return
	}

	// Parse limit - default 1000, max 50000
	limit := domain.DefaultLimit
	if limitStr := c.Query("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			h.sendError(c, 400, "invalid limit value")
			return
		}
		if limit > domain.MaxLimit {
			h.sendError(c, 400, "limit exceeds maximum of 50000")
			return
		}
	}

	// Parse metrics - optional comma-separated list
	var metricKeys []string
	if metricsStr := c.Query("metrics"); metricsStr != "" {
		metricKeys = strings.Split(metricsStr, ",")
		for i, key := range metricKeys {
			metricKeys[i] = strings.TrimSpace(key)
		}
	}

	// Build query
	telemetryQuery := domain.TelemetryQuery{
		DeviceID:   deviceID,
		From:      from,
		To:        to,
		Limit:     limit,
		MetricKeys: metricKeys,
	}

	// Call service
	results, err := h.telemetryService.QueryDeviceTelemetry(c.Request.Context(), telemetryQuery)
	if err != nil {
		h.logger.Error("query telemetry failed", "error", err)
		h.sendError(c, 500, "failed to query telemetry")
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
			Count:       r.Count,
			Source:      r.Source,
			Metrics:     r.Metrics,
		})
	}

	response := map[string]interface{}{
		"data": responseData,
		"meta": map[string]interface{}{
			"device_id": deviceID,
			"from":     from.Format(time.RFC3339),
			"to":       to.Format(time.RFC3339),
			"limit":    limit,
			"count":   len(results),
		},
	}

	if len(metricKeys) > 0 {
		response["meta"].(map[string]interface{})["metrics"] = metricKeys
	}

	c.JSON(200, response)
}

// GetChart returns chart-friendly data for a device.
func (h *Handler) GetChart(c *gin.Context) {
	// Extract device_id from path parameter
	deviceID := c.Param("device_id")
	if deviceID == "" {
		h.sendError(c, 400, "device_id is required")
		return
	}

	// Parse query parameters
	// Parse 'from' - default to 24 hours ago
	fromStr := c.Query("from")
	var from time.Time
	if fromStr != "" {
		var err error
		from, err = time.Parse(time.RFC3339, fromStr)
		if err != nil {
			h.sendError(c, 400, "invalid 'from' timestamp format")
			return
		}
	} else {
		from = time.Now().UTC().Add(-24 * time.Hour)
	}

	// Parse 'to' - default to now
	toStr := c.Query("to")
	var to time.Time
	if toStr != "" {
		var err error
		to, err = time.Parse(time.RFC3339, toStr)
		if err != nil {
			h.sendError(c, 400, "invalid 'to' timestamp format")
			return
		}
	} else {
		to = time.Now().UTC()
	}

	// Validate time range - max 3 months
	// Validate that 'from' precedes 'to'
	if !from.Before(to) {
		h.sendError(c, 400, "'from' must be before 'to'")
		return
	}
	if to.Sub(from) > domain.MaxTimeRange {
		h.sendError(c, 400, "time range exceeds maximum of 3 months")
		return
	}

	// Parse limit - default 1000, max 50000
	limit := domain.DefaultLimit
	if limitStr := c.Query("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			h.sendError(c, 400, "invalid limit value")
			return
		}
		if limit > domain.MaxLimit {
			h.sendError(c, 400, "limit exceeds maximum of 50000")
			return
		}
	}

	// Parse metrics - optional comma-separated list
	var metricKeys []string
	if metricsStr := c.Query("metrics"); metricsStr != "" {
		metricKeys = strings.Split(metricsStr, ",")
		for i, key := range metricKeys {
			metricKeys[i] = strings.TrimSpace(key)
		}
	}

	// Build query
	telemetryQuery := domain.TelemetryQuery{
		DeviceID:   deviceID,
		From:      from,
		To:        to,
		Limit:     limit,
		MetricKeys: metricKeys,
	}

	// Call service
	series, err := h.telemetryService.QueryDeviceChart(c.Request.Context(), telemetryQuery)
	if err != nil {
		h.logger.Error("query chart failed", "error", err)
		h.sendError(c, 500, "failed to query chart")
		return
	}

	// Build meta (same as GetTelemetry)
	meta := map[string]interface{}{
		"device_id": deviceID,
		"from":      from.Format(time.RFC3339),
		"to":        to.Format(time.RFC3339),
		"limit":     limit,
	}

	if len(metricKeys) > 0 {
		meta["metrics"] = metricKeys
	}

	response := ChartResponse{
		Series: series,
		Meta:   meta,
	}

	c.JSON(200, response)
}

// sendError sends a JSON error response.
func (h *Handler) sendError(c *gin.Context, status int, message string) {
	c.JSON(status, map[string]string{
		"error": message,
	})
}
