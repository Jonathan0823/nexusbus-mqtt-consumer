package httpadapter

import (
	"github.com/gin-gonic/gin"
)

// NewEngine registers HTTP routes and returns a Gin engine.
func NewEngine(h *Handler, allowedOrigins []string) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()

	// Apply CORS middleware if allowed origins configured
	if len(allowedOrigins) > 0 {
		engine.Use(corsMiddleware(allowedOrigins))
	}

	// Health and metrics routes
	engine.GET("/healthz", h.Healthz)
	engine.GET("/readyz", h.Readyz)
	engine.GET("/metrics", h.Metrics)

	// Telemetry GET routes
	engine.GET("/api/v1/devices/:device_id/telemetry", h.GetTelemetry)
	engine.GET("/api/v1/devices/:device_id/chart", h.GetChart)

	return engine
}