package httpadapter

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// corsMiddleware creates a CORS middleware with strict allowlist.
func corsMiddleware(allowedOrigins []string) gin.HandlerFunc {
	cfg := cors.DefaultConfig()
	cfg.AllowOrigins = allowedOrigins
	cfg.AllowCredentials = false // explicitly disabled
	cfg.AllowMethods = []string{"GET", "OPTIONS"}
	cfg.AllowHeaders = []string{"Content-Type", "Accept"}

	return cors.New(cfg)
}
