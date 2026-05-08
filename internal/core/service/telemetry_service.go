package service

import (
	"context"
	"sort"

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
// Uses streaming to handle large time ranges efficiently.
// limit parameter is now the LTTB downsampling target, not database row limit.
func (s *TelemetryServiceImpl) QueryDeviceChart(ctx context.Context, q domain.TelemetryQuery) ([]domain.ChartSeries, error) {
	// Use streaming repository to avoid loading all rows into memory.
	// Each row gets grouped into series during streaming.

	// Pre-allocate maps for grouping per metric.
	// Start with estimates based on limit for memory efficiency.
	targetPoints := q.Limit
	if targetPoints <= 0 {
		targetPoints = ChartTargetPoints()
	}

	seriesMap := make(map[string][]domain.ChartPoint)

	err := s.repo.StreamTelemetry(ctx, q, func(row domain.EnrichedTelemetry) error {
		if row.Metrics == nil {
			return nil
		}
		for key, val := range row.Metrics {
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
				X: row.Time.UnixMilli(),
				Y: y,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Sort and downsample each series.
	var series []domain.ChartSeries
	if len(q.MetricKeys) > 0 {
		for _, key := range q.MetricKeys {
			if pts, ok := seriesMap[key]; ok {
				// Sort points ascending by timestamp for correct chart render order.
				sort.Slice(pts, func(i, j int) bool { return pts[i].X < pts[j].X })
				pts = downsampleLTTB(pts, targetPoints)
				series = append(series, domain.ChartSeries{
					Metric: key,
					Points: pts,
				})
			}
		}
	} else {
		for key, pts := range seriesMap {
			// Sort points ascending by timestamp for correct chart render order.
			sort.Slice(pts, func(i, j int) bool { return pts[i].X < pts[j].X })
			pts = downsampleLTTB(pts, targetPoints)
			series = append(series, domain.ChartSeries{
				Metric: key,
				Points: pts,
			})
		}
	}

	return series, nil
}
