package service

import (
	"context"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/ports"
)

// TelemetryServiceImpl implements ports.TelemetryService.
type TelemetryServiceImpl struct {
	repo ports.TelemetryRepository
}

// NewTelemetryService creates a new telemetry service.
func NewTelemetryService(repo ports.TelemetryRepository) *TelemetryServiceImpl {
	return &TelemetryServiceImpl{
		repo: repo,
	}
}

// QueryDeviceTelemetry returns telemetry data for a device within a time range.
func (s *TelemetryServiceImpl) QueryDeviceTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error) {
	return s.repo.QueryTelemetry(ctx, q)
}

// QueryDeviceChart returns chart series for a device within a time range.
func (s *TelemetryServiceImpl) QueryDeviceChart(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error) {
	results, err := s.repo.QueryTelemetry(ctx, q)
	if err != nil {
		return nil, err
	}

	// Group points by metric key
	seriesMap := make(map[string][]domain.ChartPoint)
	for _, r := range results {
		if r.Metrics == nil {
			continue
		}
		for key, val := range r.Metrics {
			// Filter by requested metrics if specified
			if len(q.MetricKeys) > 0 {
				found := false
				for _, m := range q.MetricKeys {
					if m == key {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
			// Convert value to float64
			var y float64
			switch v := val.(type) {
			case float64:
				y = v
			case float32:
				y = float64(v)
			case int:
				y = float64(v)
			case int64:
				y = float64(v)
			default:
				continue
			}
			seriesMap[key] = append(seriesMap[key], domain.ChartPoint{
				X: r.Time,
				Y: y,
			})
		}
	}

	// Build series slice (preserves order if metricKeys specified)
	var series []domain.ChartSeries
	if len(q.MetricKeys) > 0 {
		for _, key := range q.MetricKeys {
			if pts, ok := seriesMap[key]; ok {
				series = append(series, domain.ChartSeries{
					Metric: key,
					Points: pts,
				})
			}
		}
	} else {
		for key, pts := range seriesMap {
			series = append(series, domain.ChartSeries{
				Metric: key,
				Points: pts,
			})
		}
	}

	return series, nil
}