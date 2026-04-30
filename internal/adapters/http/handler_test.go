package httpadapter

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/metrics"
)

func TestReadyzDoesNotExposeBackendErrors(t *testing.T) {
	t.Parallel()

	h := NewHandler(
		logging.New("error"),
		metrics.NewRecorder(),
		func() error { return errors.New("mqtt boom") },
		func() error { return errors.New("redis boom") },
		func() error { return errors.New("postgres boom") },
		false,
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	h.Readyz(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, "boom") {
		t.Fatalf("readyz leaked internal error details: %s", body)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
