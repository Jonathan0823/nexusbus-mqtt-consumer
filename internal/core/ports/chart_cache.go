package ports

import (
	"context"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
)

// ChartCache defines the interface for caching chart data in Redis.
type ChartCache interface {
	// GetChart retrieves cached chart series for a query.
	// Returns nil if not found or expired.
	GetChart(ctx context.Context, key string) ([]domain.ChartSeries, error)
	// SetChart stores chart series in cache with TTL.
	SetChart(ctx context.Context, key string, series []domain.ChartSeries, ttl time.Duration) error
	// DeleteChart removes cached chart data.
	DeleteChart(ctx context.Context, key string) error
}
