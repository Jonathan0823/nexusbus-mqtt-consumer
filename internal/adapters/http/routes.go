package httpadapter

import "net/http"

// NewMux registers HTTP routes and returns a ServeMux.
func NewMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.Healthz)
	mux.HandleFunc("/readyz", h.Readyz)
	mux.HandleFunc("/metrics", h.Metrics)
	return mux
}
