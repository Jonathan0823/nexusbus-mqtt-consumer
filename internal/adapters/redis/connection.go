package redis

import (
	"context"
	"fmt"
	"time"

	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"

	"github.com/go-redis/redis/v8"
)

// NewClient creates a new Redis client.
func NewClient(cfg config.RedisConfig, logger *logging.Logger) (*redis.Client, error) {
	options, err := redis.ParseURL(cfg.Addr)
	if err != nil {
		options = &redis.Options{Addr: cfg.Addr}
	}
	options.Password = cfg.Password
	options.DB = cfg.DB

	client := redis.NewClient(options)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis connect failed: %w", err)
	}

	logger.Info("redis client connected", "addr", cfg.Addr)
	return client, nil
}
