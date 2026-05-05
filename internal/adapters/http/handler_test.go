package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/metrics"
)

// mockTelemetryService is a mock implementation of TelemetryService for testing.
type mockTelemetryService struct {
	telemetry []domain.EnrichedTelemetry
	err       error
}

func (m *mockTelemetryService) QueryDeviceTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error) {
	return m.telemetry, m.err
}

func TestReadyzDoesNotExposeBackendErrors(t *testing.T) {
	t.Parallel()

	h := NewHandler(
		logging.New("error"),
		metrics.NewRecorder(),
		nil,
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

func TestGetTelemetry_ValidRequest(t *testing.T) {
	t.Parallel()

	mockSvc := &mockTelemetryService{
		telemetry: []domain.EnrichedTelemetry{
			{
				Time:        time.Now().UTC(),
				DeviceID:    "office-eng",
				ProfileID:   "power_meter_9",
				Metrics:     map[string]any{"voltage": 227.5, "current": 0.625},
			},
		},
	}

	h := NewHandler(
		logging.New("error"),
		metrics.NewRecorder(),
		mockSvc,
		nil,
		nil,
		nil,
		true,
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/devices/office-eng/telemetry?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z&limit=100", nil)
	h.GetTelemetry(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Check response structure
	data, ok := response["data"].([]interface{})
	if !ok || len(data) != 1 {
		t.Fatalf("expected 1 data item, got %v", response["data"])
	}

	meta, ok := response["meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected meta in response")
	}

	if meta["device_id"] != "office-eng" {
		t.Fatalf("expected device_id office-eng, got %v", meta["device_id"])
	}

	// Check that raw_payload and idempotency_key are not in the response
	dataMap, ok := data[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected data[0] to be a map")
	}

	if _, exists := dataMap["raw_payload"]; exists {
		t.Fatal("raw_payload should not be in response")
	}

	if _, exists := dataMap["idempotency_key"]; exists {
		t.Fatal("idempotency_key should not be in response")
	}
}

func TestGetTelemetry_MissingDeviceID(t *testing.T) {
	t.Parallel()

	h := NewHandler(
		logging.New("error"),
		metrics.NewRecorder(),
		nil,
		nil,
		nil,
		nil,
		true,
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/devices//telemetry", nil)
	h.GetTelemetry(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if response["error"] != "device_id is required" {
		t.Fatalf("expected 'device_id is required', got %v", response["error"])
	}
}

func TestGetTelemetry_InvalidTimeRange(t *testing.T) {
	t.Parallel()

	h := NewHandler(
		logging.New("error"),
		metrics.NewRecorder(),
		nil,
		nil,
		nil,
		nil,
		true,
	)

	// 4 months range exceeds 3 month limit
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/devices/office-eng/telemetry?from=2025-01-01T00:00:00Z&to=2025-05-01T00:00:00Z", nil)
	h.GetTelemetry(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if !strings.Contains(response["error"], "exceeds maximum") {
		t.Fatalf("expected time range error, got %v", response["error"])
	}
}

func TestGetTelemetry_InvalidLimit(t *testing.T) {
	t.Parallel()

	h := NewHandler(
		logging.New("error"),
		metrics.NewRecorder(),
		nil,
		nil,
		nil,
		nil,
		true,
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/devices/office-eng/telemetry?limit=invalid", nil)
	h.GetTelemetry(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetTelemetry_LimitExceedsMax(t *testing.T) {
	t.Parallel()

	h := NewHandler(
		logging.New("error"),
		metrics.NewRecorder(),
		nil,
		nil,
		nil,
		nil,
		true,
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/devices/office-eng/telemetry?limit=100000", nil)
	h.GetTelemetry(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if !strings.Contains(response["error"], "exceeds maximum") {
		t.Fatalf("expected limit error, got %v", response["error"])
	}
}
