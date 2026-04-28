package httpadapter

import (
	"encoding/json"
	"fmt"
	"net/http"

	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/metrics"
)

// Handler holds HTTP handlers and dependencies.
type Handler struct {
	logger         *logging.Logger
	metrics        *metrics.Recorder
	mqttPing       func() error
	redisPing      func() error
	postgresPing   func() error
	profilesLoaded bool
}

// NewHandler creates a new HTTP handler.
func NewHandler(
	logger *logging.Logger,
	metrics *metrics.Recorder,
	mqttPing func() error,
	redisPing func() error,
	postgresPing func() error,
	profilesLoaded bool,
) *Handler {
	return &Handler{
		logger:         logger,
		metrics:        metrics,
		mqttPing:       mqttPing,
		redisPing:      redisPing,
		postgresPing:   postgresPing,
		profilesLoaded: profilesLoaded,
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
			checks["mqtt"] = "unhealthy: " + err.Error()
			allHealthy = false
		} else {
			checks["mqtt"] = "healthy"
		}
	}

	// Check Redis
	if h.redisPing != nil {
		if err := h.redisPing(); err != nil {
			checks["redis"] = "unhealthy: " + err.Error()
			allHealthy = false
		} else {
			checks["redis"] = "healthy"
		}
	}

	// Check PostgreSQL
	if h.postgresPing != nil {
		if err := h.postgresPing(); err != nil {
			checks["postgres"] = "unhealthy: " + err.Error()
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

// ServeHTTP implements http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		h.Healthz(w, r)
	case "/readyz":
		h.Readyz(w, r)
	case "/metrics":
		h.Metrics(w, r)
	default:
		http.NotFound(w, r)
	}
}
