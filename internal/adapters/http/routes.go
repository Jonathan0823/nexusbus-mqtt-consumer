package httpadapter

import (
	"github.com/gin-gonic/gin"
)

// NewEngine registers HTTP routes and returns a Gin engine.
// basePath is prepended to all routes (e.g. "/telemetry"). Empty basePath mounts at root.
func NewEngine(h *Handler, allowedOrigins []string, basePath string) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()

	if len(allowedOrigins) > 0 {
		engine.Use(corsMiddleware(allowedOrigins))
	}

	rg := engine.Group("")
	if basePath != "" {
		rg = engine.Group(basePath)
	}

	registerRoutes(rg, h)

	return engine
}

func registerRoutes(rg *gin.RouterGroup, h *Handler) {
	rg.GET("/healthz", h.Healthz)
	rg.GET("/readyz", h.Readyz)
	rg.GET("/metrics", h.Metrics)
	rg.GET("/api/v1/devices/:device_id/telemetry", h.GetTelemetry)
	rg.GET("/api/v1/devices/:device_id/chart", h.GetChart)
}
