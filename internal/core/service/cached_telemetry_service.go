package service

import (
	"context"
	"fmt"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/ports"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// CachedTelemetryService wraps TelemetryService with Redis caching for chart queries.
type CachedTelemetryService struct {
	delegate  ports.TelemetryService
	cache     ports.ChartCache
	logger    *logging.Logger
	cacheTTL  time.Duration
}

// NewCachedTelemetryService creates a new cached telemetry service.
func NewCachedTelemetryService(
	delegate ports.TelemetryService,
	cache ports.ChartCache,
	logger *logging.Logger,
	cacheTTL time.Duration,
) *CachedTelemetryService {
	return &CachedTelemetryService{
		delegate:  delegate,
		cache:     cache,
		logger:    logger,
		cacheTTL:  cacheTTL,
	}
}

// QueryDeviceTelemetry returns telemetry data (not cached, direct delegate).
func (s *CachedTelemetryService) QueryDeviceTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error) {
	return s.delegate.QueryDeviceTelemetry(ctx, q)
}

// QueryDeviceChart returns cached chart data if available, otherwise queries and caches.
func (s *CachedTelemetryService) QueryDeviceChart(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error) {
	// Build cache key from query parameters
	cacheKey := buildChartCacheKey(q)

	// Try cache first
	if s.cache != nil {
		if cached, err := s.cache.GetChart(ctx, cacheKey); err != nil {
			s.logger.Warn("chart cache get failed", "error", err)
		} else if cached != nil {
			s.logger.Debug("chart cache hit", "key", cacheKey)
			return cached, nil
		}
	}

	// Cache miss - query from delegate
	s.logger.Debug("chart cache miss", "key", cacheKey)
	series, err := s.delegate.QueryDeviceChart(ctx, q)
	if err != nil {
		return nil, err
	}

	// Store in cache
	if s.cache != nil && len(series) > 0 {
		if err := s.cache.SetChart(ctx, cacheKey, series, s.cacheTTL); err != nil {
			s.logger.Warn("chart cache set failed", "error", err)
		}
	}

	return series, nil
}

// buildChartCacheKey creates a deterministic cache key from query parameters.
func buildChartCacheKey(q domain.TelemetryQuery) string {
	// Format: device|from_rfc3339|to_rfc3339|target_points|metrics_sorted
	fromStr := q.From.Format(time.RFC3339)
	toStr := q.To.Format(time.RFC3339)

	metricsStr := ""
	if len(q.MetricKeys) > 0 {
		// Sort metrics for consistent key
		// Use a simple approach - join with comma
		metricsStr = fmt.Sprintf("m:%s", joinStrings(q.MetricKeys))
	}

	return fmt.Sprintf("%s|%s|%s|%d%s", q.DeviceID, fromStr, toStr, q.Limit, metricsStr)
}

// joinStrings joins string slice with comma.
func joinStrings(strs []string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += "," + strs[i]
	}
	return result
}