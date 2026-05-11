package httpadapter

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"modbus-mqtt-consumer/internal/platform/metrics"
)

func TestNewEngine_RootPath(t *testing.T) {
	t.Parallel()

	h := NewHandler(nil, metrics.NewRecorder(), nil, nil, nil, nil, true)
	engine := NewEngine(h, nil, "")

	routes := []struct {
		method, path string
		wantStatus   int
	}{
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodGet, "/readyz", http.StatusOK},
		{http.MethodGet, "/metrics", http.StatusOK},
	}
	for _, r := range routes {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(r.method, r.path, nil)
		engine.ServeHTTP(w, req)
		if w.Code != r.wantStatus {
			t.Errorf("GET %s = %d, want %d", r.path, w.Code, r.wantStatus)
		}
	}
}

func TestNewEngine_PrefixedPath(t *testing.T) {
	t.Parallel()

	h := NewHandler(nil, metrics.NewRecorder(), nil, nil, nil, nil, true)
	engine := NewEngine(h, nil, "/telemetry")

	routes := []struct {
		method, path       string
		wantStatus         int
	}{
		{http.MethodGet, "/telemetry/healthz", http.StatusOK},
		{http.MethodGet, "/telemetry/readyz", http.StatusOK},
		{http.MethodGet, "/telemetry/metrics", http.StatusOK},
		// Root paths should not match when base path is set
		{http.MethodGet, "/healthz", http.StatusNotFound},
		{http.MethodGet, "/readyz", http.StatusNotFound},
		{http.MethodGet, "/metrics", http.StatusNotFound},
	}
	for _, r := range routes {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(r.method, r.path, nil)
		engine.ServeHTTP(w, req)
		if w.Code != r.wantStatus {
			t.Errorf("GET %s = %d, want %d", r.path, w.Code, r.wantStatus)
		}
	}
}

func TestNewEngine_DeepBasePath(t *testing.T) {
	t.Parallel()

	h := NewHandler(nil, metrics.NewRecorder(), nil, nil, nil, nil, true)
	engine := NewEngine(h, nil, "/api/v1/telemetry")

	routes := []struct {
		method, path       string
		wantStatus         int
	}{
		{http.MethodGet, "/api/v1/telemetry/healthz", http.StatusOK},
		{http.MethodGet, "/api/v1/telemetry/readyz", http.StatusOK},
		// Root paths should not match when base path is set
		{http.MethodGet, "/healthz", http.StatusNotFound},
		{http.MethodGet, "/readyz", http.StatusNotFound},
	}
	for _, r := range routes {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(r.method, r.path, nil)
		engine.ServeHTTP(w, req)
		if w.Code != r.wantStatus {
			t.Errorf("GET %s = %d, want %d", r.path, w.Code, r.wantStatus)
		}
	}
}