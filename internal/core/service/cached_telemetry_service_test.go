package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// mockChartCache is a mock implementation of ports.ChartCache for testing.
type mockChartCache struct {
	getFunc  func(ctx context.Context, key string) ([]domain.ChartSeries, error)
	setFunc  func(ctx context.Context, key string, series []domain.ChartSeries, ttl time.Duration) error
	getCalls int
	setCalls int
}

func (m *mockChartCache) GetChart(ctx context.Context, key string) ([]domain.ChartSeries, error) {
	m.getCalls++
	if m.getFunc != nil {
		return m.getFunc(ctx, key)
	}
	return nil, nil
}

func (m *mockChartCache) SetChart(ctx context.Context, key string, series []domain.ChartSeries, ttl time.Duration) error {
	m.setCalls++
	if m.setFunc != nil {
		return m.setFunc(ctx, key, series, ttl)
	}
	return nil
}

func (m *mockChartCache) DeleteChart(ctx context.Context, key string) error {
	return nil
}

// mockTelemetryService is a mock implementation of ports.TelemetryService for testing.
type mockTelemetryService struct {
	chartFunc func(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error)
	teleFunc  func(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error)
}

func (m *mockTelemetryService) QueryDeviceTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error) {
	if m.teleFunc != nil {
		return m.teleFunc(ctx, q)
	}
	return nil, nil
}

func (m *mockTelemetryService) QueryDeviceChart(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error) {
	if m.chartFunc != nil {
		return m.chartFunc(ctx, q)
	}
	return nil, nil
}

func TestCachedTelemetryService_ChartCacheHit(t *testing.T) {
	t.Parallel()

	// Setup mock cache that returns cached data
	cache := &mockChartCache{
		getFunc: func(ctx context.Context, key string) ([]domain.ChartSeries, error) {
			return []domain.ChartSeries{
				{Metric: "voltage", Points: []domain.ChartPoint{{X: 1000, Y: 220.5}}},
			}, nil
		},
	}

	// Setup mock delegate that should NOT be called
	delegate := &mockTelemetryService{
		chartFunc: func(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error) {
			t.Fatal("delegate should not be called on cache hit")
			return nil, nil
		},
	}

	logger := logging.New("error")
	svc := NewCachedTelemetryService(delegate, cache, logger, 30*time.Second)

	query := domain.TelemetryQuery{
		DeviceID: "device-1",
		From:     time.Now().Add(-1 * time.Hour),
		To:       time.Now(),
		Limit:    500,
	}

	result, err := svc.QueryDeviceChart(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result))
	}

	if result[0].Metric != "voltage" {
		t.Fatalf("expected metric 'voltage', got %s", result[0].Metric)
	}

	if cache.getCalls != 1 {
		t.Fatalf("expected 1 cache get call, got %d", cache.getCalls)
	}

	// Delegate should not be called
	if cache.setCalls != 0 {
		t.Fatalf("expected 0 cache set calls on hit, got %d", cache.setCalls)
	}
}

func TestCachedTelemetryService_ChartCacheMiss(t *testing.T) {
	t.Parallel()

	// Setup mock cache that returns nil (cache miss)
	cache := &mockChartCache{
		getFunc: func(ctx context.Context, key string) ([]domain.ChartSeries, error) {
			return nil, nil // cache miss
		},
	}

	// Setup mock delegate that returns data
	delegate := &mockTelemetryService{
		chartFunc: func(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error) {
			return []domain.ChartSeries{
				{Metric: "current", Points: []domain.ChartPoint{{X: 1000, Y: 1.5}}},
			}, nil
		},
	}

	logger := logging.New("error")
	svc := NewCachedTelemetryService(delegate, cache, logger, 30*time.Second)

	query := domain.TelemetryQuery{
		DeviceID: "device-1",
		From:     time.Now().Add(-1 * time.Hour),
		To:       time.Now(),
		Limit:    500,
	}

	result, err := svc.QueryDeviceChart(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result))
	}

	if result[0].Metric != "current" {
		t.Fatalf("expected metric 'current', got %s", result[0].Metric)
	}

	// Cache should have been called to get (miss) and set (store)
	if cache.getCalls != 1 {
		t.Fatalf("expected 1 cache get call, got %d", cache.getCalls)
	}
	if cache.setCalls != 1 {
		t.Fatalf("expected 1 cache set call, got %d", cache.setCalls)
	}
}

func TestCachedTelemetryService_ChartCacheError(t *testing.T) {
	t.Parallel()

	// Setup mock cache that returns error on get
	cache := &mockChartCache{
		getFunc: func(ctx context.Context, key string) ([]domain.ChartSeries, error) {
			return nil, errors.New("redis error")
		},
	}

	// Setup mock delegate that returns data
	delegate := &mockTelemetryService{
		chartFunc: func(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error) {
			return []domain.ChartSeries{
				{Metric: "power", Points: []domain.ChartPoint{{X: 1000, Y: 500}}},
			}, nil
		},
	}

	logger := logging.New("error")
	svc := NewCachedTelemetryService(delegate, cache, logger, 30*time.Second)

	query := domain.TelemetryQuery{
		DeviceID: "device-1",
		From:     time.Now().Add(-1 * time.Hour),
		To:       time.Now(),
		Limit:    500,
	}

	result, err := svc.QueryDeviceChart(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still return data from delegate when cache fails
	if len(result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result))
	}
}

func TestCachedTelemetryService_TelemetryNotCached(t *testing.T) {
	t.Parallel()

	// Telemetry queries should go directly to delegate (not cached)
	cache := &mockChartCache{}

	delegate := &mockTelemetryService{
		teleFunc: func(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error) {
			return []domain.EnrichedTelemetry{
				{DeviceID: "device-1", Time: time.Now()},
			}, nil
		},
	}

	logger := logging.New("error")
	svc := NewCachedTelemetryService(delegate, cache, logger, 30*time.Second)

	query := domain.TelemetryQuery{
		DeviceID: "device-1",
		From:     time.Now().Add(-1 * time.Hour),
		To:       time.Now(),
		Limit:    100,
	}

	result, err := svc.QueryDeviceTelemetry(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 telemetry, got %d", len(result))
	}

	// Cache should not be called for telemetry
	if cache.getCalls != 0 || cache.setCalls != 0 {
		t.Fatalf("cache should not be called for telemetry, got get=%d set=%d", cache.getCalls, cache.setCalls)
	}
}

func TestBuildChartCacheKey_Deterministic(t *testing.T) {
	t.Parallel()

	query := domain.TelemetryQuery{
		DeviceID:   "device-1",
		From:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		To:         time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit:      500,
		MetricKeys: []string{"voltage", "current"},
	}

	key1 := buildChartCacheKey(query)
	key2 := buildChartCacheKey(query)

	if key1 != key2 {
		t.Fatalf("cache key should be deterministic, got %s and %s", key1, key2)
	}

	// Verify key contains expected components
	if key1 == "" {
		t.Fatal("cache key should not be empty")
	}
}

func TestBuildChartCacheKey_DifferentQueriesDifferentKeys(t *testing.T) {
	t.Parallel()

	query1 := domain.TelemetryQuery{
		DeviceID: "device-1",
		From:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		To:       time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit:    500,
	}

	query2 := domain.TelemetryQuery{
		DeviceID: "device-2", // different device
		From:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		To:       time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit:    500,
	}

	key1 := buildChartCacheKey(query1)
	key2 := buildChartCacheKey(query2)

	if key1 == key2 {
		t.Fatalf("different queries should produce different keys")
	}
}

func TestCachedTelemetryService_NilCache(t *testing.T) {
	t.Parallel()

	// Test with nil cache - should still work (fallback to delegate)
	delegate := &mockTelemetryService{
		chartFunc: func(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error) {
			return []domain.ChartSeries{
				{Metric: "temp", Points: []domain.ChartPoint{{X: 1000, Y: 25.5}}},
			}, nil
		},
	}

	logger := logging.New("error")
	svc := NewCachedTelemetryService(delegate, nil, logger, 30*time.Second)

	query := domain.TelemetryQuery{
		DeviceID: "device-1",
		From:     time.Now().Add(-1 * time.Hour),
		To:       time.Now(),
		Limit:    500,
	}

	result, err := svc.QueryDeviceChart(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result))
	}
}
