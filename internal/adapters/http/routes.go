package httpadapter

import (
	"github.com/gin-gonic/gin"
)

// NewEngine registers HTTP routes and returns a Gin engine.
func NewEngine(h *Handler) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()

	// Health and metrics routes
	engine.GET("/healthz", h.Healthz)
	engine.GET("/readyz", h.Readyz)
	engine.GET("/metrics", h.Metrics)

	// Telemetry GET route
	engine.GET("/api/v1/devices/:device_id/telemetry", h.GetTelemetry)

	return engine
}