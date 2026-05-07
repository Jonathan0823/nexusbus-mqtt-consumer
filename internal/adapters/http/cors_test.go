package httpadapter

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestCORSMiddleware_AllowedOrigin(t *testing.T) {
	t.Parallel()

	allowedOrigins := []string{"http://localhost:3000", "http://127.0.0.1:3000"}

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(corsMiddleware(allowedOrigins))
	engine.GET("/healthz", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	// Request with allowed origin header
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatalf("expected Access-Control-Allow-Origin header, got: %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestCORSMiddleware_DisallowedOrigin(t *testing.T) {
	t.Parallel()

	allowedOrigins := []string{"http://localhost:3000"}

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(corsMiddleware(allowedOrigins))
	engine.GET("/healthz", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	// Request with disallowed origin header
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://evil-site.com")
	engine.ServeHTTP(w, req)

	// gin-contrib/cors blocks disallowed origins with 403
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for disallowed origin, got %d", w.Code)
	}
}

func TestCORSMiddleware_PreflightAllowed(t *testing.T) {
	t.Parallel()

	allowedOrigins := []string{"http://localhost:3000"}

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(corsMiddleware(allowedOrigins))
	engine.GET("/healthz", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	// OPTIONS preflight with allowed origin
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/healthz", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "GET")
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 NoContent, got %d", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatalf("expected Access-Control-Allow-Origin, got: %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
	// gin-contrib returns "GET,OPTIONS" without space
	if w.Header().Get("Access-Control-Allow-Methods") != "GET,OPTIONS" {
		t.Fatalf("expected Access-Control-Allow-Methods, got: %s", w.Header().Get("Access-Control-Allow-Methods"))
	}
}

func TestCORSMiddleware_PreflightDisallowed(t *testing.T) {
	t.Parallel()

	allowedOrigins := []string{"http://localhost:3000"}

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(corsMiddleware(allowedOrigins))
	engine.GET("/healthz", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	// OPTIONS preflight with disallowed origin
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/healthz", nil)
	req.Header.Set("Origin", "http://evil-site.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for disallowed preflight, got %d", w.Code)
	}
}

func TestCORSMiddleware_NoOriginHeader(t *testing.T) {
	t.Parallel()

	allowedOrigins := []string{"http://localhost:3000"}

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(corsMiddleware(allowedOrigins))
	engine.GET("/healthz", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	// Request without Origin header (same-origin request)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	// Should NOT set CORS headers for same-origin requests
	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("expected no Access-Control-Allow-Origin for same-origin request, got: %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
}