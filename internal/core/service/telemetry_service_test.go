package service

import (
	"context"
	"testing"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
)

type mockTelemetryRepo struct {
	telemetry []domain.EnrichedTelemetry
}

func (m *mockTelemetryRepo) InsertBatchIdempotent(ctx context.Context, rows []domain.EnrichedTelemetry) (int, error) {
	return 0, nil
}

func (m *mockTelemetryRepo) GetLatest(ctx context.Context, deviceID string) (*domain.EnrichedTelemetry, error) {
	return nil, nil
}

func (m *mockTelemetryRepo) QueryRange(ctx context.Context, q domain.TelemetryRangeQuery) ([]domain.EnrichedTelemetry, error) {
	return nil, nil
}

func (m *mockTelemetryRepo) QueryTelemetry(ctx context.Context, q domain.TelemetryQuery) ([]domain.EnrichedTelemetry, error) {
	return m.telemetry, nil
}

func (m *mockTelemetryRepo) StreamTelemetry(ctx context.Context, q domain.TelemetryQuery, fn func(domain.EnrichedTelemetry) error) error {
	for _, t := range m.telemetry {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockTelemetryRepo) Ping(ctx context.Context) error { return nil }

func TestDownsampleLTTB_PreservesEndpointsAndLimit(t *testing.T) {
	t.Parallel()

	points := make([]domain.ChartPoint, 0, 1000)
	for i := 0; i < 1000; i++ {
		points = append(points, domain.ChartPoint{X: int64(i), Y: float64(i % 100)})
	}

	resampled := downsampleLTTB(points, 500)
	if len(resampled) != 500 {
		t.Fatalf("expected 500 points, got %d", len(resampled))
	}
	if resampled[0] != points[0] {
		t.Fatalf("expected first point preserved")
	}
	if resampled[len(resampled)-1] != points[len(points)-1] {
		t.Fatalf("expected last point preserved")
	}
	for i := 1; i < len(resampled); i++ {
		if resampled[i].X < resampled[i-1].X {
			t.Fatalf("expected ascending order at index %d", i)
		}
	}
}

func TestDownsampleLTTB_SmallSeriesUnchanged(t *testing.T) {
	t.Parallel()

	points := make([]domain.ChartPoint, 0, 10)
	for i := 0; i < 10; i++ {
		points = append(points, domain.ChartPoint{X: int64(i), Y: float64(i)})
	}

	resampled := downsampleLTTB(points, 500)
	if len(resampled) != len(points) {
		t.Fatalf("expected unchanged length %d, got %d", len(points), len(resampled))
	}
}

func TestQueryDeviceChart_DownsamplesSeries(t *testing.T) {
	t.Parallel()

	telemetry := make([]domain.EnrichedTelemetry, 0, 1000)
	for i := 0; i < 1000; i++ {
		telemetry = append(telemetry, domain.EnrichedTelemetry{
			Time:     time.UnixMilli(int64(i)).UTC(),
			DeviceID: "office-eng",
			Metrics:  map[string]any{"energy": float64(i)},
		})
	}

	svc := NewTelemetryService(&mockTelemetryRepo{telemetry: telemetry})
	series, err := svc.QueryDeviceChart(context.Background(), domain.TelemetryQuery{DeviceID: "office-eng"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(series))
	}
	if len(series[0].Points) != 500 {
		t.Fatalf("expected 500 points after downsampling, got %d", len(series[0].Points))
	}
	if series[0].Points[0].X > series[0].Points[len(series[0].Points)-1].X {
		t.Fatalf("expected ascending chart points")
	}
}
