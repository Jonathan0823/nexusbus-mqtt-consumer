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

	"github.com/gin-gonic/gin"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/metrics"
)

// mockTelemetryService is a mock implementation of TelemetryService for testing.
type mockTelemetryService struct {
	telemetry []domain.EnrichedTelemetry
	chart     []domain.ChartSeries
	err       error
}

func (m *mockTelemetryService) QueryDeviceTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error) {
	return m.telemetry, m.err
}

func (m *mockTelemetryService) QueryDeviceChart(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error) {
	return m.chart, m.err
}

// setupTestContext creates a Gin test context with the given path params.
func setupTestContext(deviceID, queryPath string) (c *gin.Context, w *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, queryPath, nil)
	if deviceID != "" {
		c.Params = gin.Params{
			{Key: "device_id", Value: deviceID},
		}
	}
	return c, w
}

// setupTestContextWithCanceledCtx creates a test context with a canceled parent context.
func setupTestContextWithCanceledCtx(deviceID, queryPath string) (c *gin.Context, w *httptest.ResponseRecorder, cancel func()) {
	gin.SetMode(gin.TestMode)
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	ctx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx() // cancel immediately so the context is already canceled
	c.Request = httptest.NewRequest(http.MethodGet, queryPath, nil).WithContext(ctx)
	if deviceID != "" {
		c.Params = gin.Params{
			{Key: "device_id", Value: deviceID},
		}
	}
	return c, w, cancelCtx
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

	c, w := setupTestContext("", "/readyz")
	h.Readyz(c)

	body := w.Body.String()
	if strings.Contains(body, "boom") {
		t.Fatalf("readyz leaked internal error details: %s", body)
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestGetTelemetry_ValidRequest(t *testing.T) {
	t.Parallel()

	mockSvc := &mockTelemetryService{
		telemetry: []domain.EnrichedTelemetry{
			{
				Time:      time.Now().UTC(),
				DeviceID:  "office-eng",
				ProfileID: "power_meter_9",
				Metrics:   map[string]any{"voltage": 227.5, "current": 0.625},
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

	c, w := setupTestContext("office-eng", "/api/v1/devices/office-eng/telemetry?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z&limit=100")
	h.GetTelemetry(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
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

	c, w := setupTestContext("", "/api/v1/devices//telemetry")
	h.GetTelemetry(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
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
	c, w := setupTestContext("office-eng", "/api/v1/devices/office-eng/telemetry?from=2025-01-01T00:00:00Z&to=2025-05-01T00:00:00Z")
	h.GetTelemetry(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
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

	c, w := setupTestContext("office-eng", "/api/v1/devices/office-eng/telemetry?limit=invalid")
	h.GetTelemetry(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
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

	c, w := setupTestContext("office-eng", "/api/v1/devices/office-eng/telemetry?limit=100000")
	h.GetTelemetry(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if !strings.Contains(response["error"], "exceeds maximum") {
		t.Fatalf("expected limit error, got %v", response["error"])
	}
}

func TestGetChart_ValidRequest(t *testing.T) {
	t.Parallel()

	mockSvc := &mockTelemetryService{
		chart: []domain.ChartSeries{
			{
				Metric: "voltage",
				Points: []domain.ChartPoint{
					{X: time.Now().UTC().UnixMilli(), Y: 227.5},
				},
			},
			{
				Metric: "current",
				Points: []domain.ChartPoint{
					{X: time.Now().UTC().UnixMilli(), Y: 0.625},
				},
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

	c, w := setupTestContext("office-eng", "/api/v1/devices/office-eng/chart?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z&target_points=100")
	h.GetChart(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Check response structure
	series, ok := response["data"].([]interface{})
	if !ok || len(series) != 2 {
		t.Fatalf("expected 2 series, got %v", response["data"])
	}

	// Check voltage series
	voltageSeries, ok := series[0].(map[string]any)
	if !ok || voltageSeries["metric"] != "voltage" {
		t.Fatalf("expected voltage series first, got %v", series[0])
	}
	voltagePoints, ok := voltageSeries["points"].([]interface{})
	if !ok || len(voltagePoints) != 1 {
		t.Fatalf("expected 1 voltage point, got %v", voltagePoints)
	}
	point, ok := voltagePoints[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected point to be a map, got %T", voltagePoints[0])
	}
	if _, ok := point["x"].(float64); !ok {
		t.Fatalf("expected x to be numeric unix timestamp, got %T", point["x"])
	}

	// Check current series
	currentSeries, ok := series[1].(map[string]any)
	if !ok || currentSeries["metric"] != "current" {
		t.Fatalf("expected current series second, got %v", series[1])
	}

	// Check meta
	meta, ok := response["meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected meta in response")
	}

	if meta["device_id"] != "office-eng" {
		t.Fatalf("expected device_id office-eng, got %v", meta["device_id"])
	}
}

func TestGetChart_MissingDeviceID(t *testing.T) {
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

	c, w := setupTestContext("", "/api/v1/devices//chart")
	h.GetChart(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if response["error"] != "device_id is required" {
		t.Fatalf("expected 'device_id is required', got %v", response["error"])
	}
}

func TestGetChart_FilterByMetrics(t *testing.T) {
	t.Parallel()

	mockSvc := &mockTelemetryService{
		chart: []domain.ChartSeries{
			{
				Metric: "voltage",
				Points: []domain.ChartPoint{
					{X: time.Now().UTC().UnixMilli(), Y: 227.5},
				},
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

	// Request only voltage metric
	c, w := setupTestContext("office-eng", "/api/v1/devices/office-eng/chart?metrics=voltage")
	h.GetChart(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Should only have voltage series
	series, ok := response["data"].([]interface{})
	if !ok || len(series) != 1 {
		t.Fatalf("expected 1 series, got %v", response["data"])
	}

	voltageSeries, ok := series[0].(map[string]any)
	if !ok || voltageSeries["metric"] != "voltage" {
		t.Fatalf("expected only voltage series, got %v", series[0])
	}
}

func TestGetTelemetry_RequestCanceled(t *testing.T) {
	t.Parallel()

	mockSvc := &mockTelemetryService{
		err: context.Canceled,
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

	c, w, cancel := setupTestContextWithCanceledCtx("office-eng", "/api/v1/devices/office-eng/telemetry")
	defer cancel()
	h.GetTelemetry(c)

	if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
		t.Fatalf("expected no response on cancel, got %d", w.Code)
	}
}

func TestGetTelemetry_ServerError(t *testing.T) {
	t.Parallel()

	mockSvc := &mockTelemetryService{
		err: errors.New("db connection lost"),
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

	c, w := setupTestContext("office-eng", "/api/v1/devices/office-eng/telemetry")
	h.GetTelemetry(c)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for server error, got %d", w.Code)
	}
}

func TestGetChart_RequestCanceled(t *testing.T) {
	t.Parallel()

	mockSvc := &mockTelemetryService{
		err: context.Canceled,
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

	c, w, cancel := setupTestContextWithCanceledCtx("office-eng", "/api/v1/devices/office-eng/chart")
	defer cancel()
	h.GetChart(c)

	if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
		t.Fatalf("expected no response on cancel, got %d", w.Code)
	}
}

func TestGetChart_ServerError(t *testing.T) {
	t.Parallel()

	mockSvc := &mockTelemetryService{
		err: errors.New("db connection lost"),
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

	c, w := setupTestContext("office-eng", "/api/v1/devices/office-eng/chart")
	h.GetChart(c)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for server error, got %d", w.Code)
	}
}
