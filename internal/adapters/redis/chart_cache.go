package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// ChartCacheAdapter implements ports.ChartCache using Redis.
type ChartCacheAdapter struct {
	client     *redis.Client
	keyPrefix  string
	logger     *logging.Logger
}

// NewChartCache creates a new Redis-backed chart cache.
func NewChartCache(client *redis.Client, logger *logging.Logger) *ChartCacheAdapter {
	return &ChartCacheAdapter{
		client:    client,
		keyPrefix: "chart_cache:v1:",
		logger:    logger,
	}
}

// GetChart retrieves cached chart series for a query.
func (c *ChartCacheAdapter) GetChart(ctx context.Context, key string) ([]domain.ChartSeries, error) {
	fullKey := c.keyPrefix + key

	data, err := c.client.Get(ctx, fullKey).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}

	var series []domain.ChartSeries
	if err := json.Unmarshal(data, &series); err != nil {
		return nil, fmt.Errorf("unmarshal chart: %w", err)
	}

	c.logger.Debug("chart cache hit", "key", key)
	return series, nil
}

// SetChart stores chart series in cache with TTL.
func (c *ChartCacheAdapter) SetChart(ctx context.Context, key string, series []domain.ChartSeries, ttl time.Duration) error {
	fullKey := c.keyPrefix + key

	data, err := json.Marshal(series)
	if err != nil {
		return fmt.Errorf("marshal chart: %w", err)
	}

	if err := c.client.Set(ctx, fullKey, data, ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}

	c.logger.Debug("chart cache set", "key", key, "ttl", ttl)
	return nil
}

// DeleteChart removes cached chart data.
func (c *ChartCacheAdapter) DeleteChart(ctx context.Context, key string) error {
	fullKey := c.keyPrefix + key

	if err := c.client.Del(ctx, fullKey).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}

	c.logger.Debug("chart cache delete", "key", key)
	return nil
}

// Ping checks Redis connectivity.
func (c *ChartCacheAdapter) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close closes the Redis client.
func (c *ChartCacheAdapter) Close() error {
	return c.client.Close()
}