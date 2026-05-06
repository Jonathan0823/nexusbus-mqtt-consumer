package httpadapter

import "net/http"

// NewMux registers HTTP routes and returns a ServeMux.
func NewMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.Healthz)
	mux.HandleFunc("/readyz", h.Readyz)
	mux.HandleFunc("/metrics", h.Metrics)

	// Telemetry GET routes - use prefix matching, handle method/device_id in handler
	mux.HandleFunc("GET /api/v1/devices/", h.GetTelemetry)

	return mux
}
