package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// NewClient creates a new Redis client.
func NewClient(cfg Config, logger *logging.Logger) (*redis.Client, error) {
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

// NewConnection is a compatibility shim that creates both client and buffer.
// Deprecated: use NewClient + NewStreamBuffer instead.
func NewConnection(cfg Config, logger *logging.Logger) (*StreamBuffer, error) {
	client, err := NewClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	return NewStreamBuffer(client, cfg, logger)
}
