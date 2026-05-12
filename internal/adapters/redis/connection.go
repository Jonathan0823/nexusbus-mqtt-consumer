package redis

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"

	"github.com/go-redis/redis/v8"
)

// redactHost strips credentials from a URL string.
func redactHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<redacted>"
	}
	if u.User != nil {
		u.User = url.UserPassword("", "")
	}
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// NewClient creates a new Redis client.
func NewClient(cfg config.RedisConfig, logger *logging.Logger) (*redis.Client, error) {
	redacted := redactHost(cfg.Addr)
	options, err := redis.ParseURL(cfg.Addr)
	if err != nil {
		options = &redis.Options{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
		}
	}

	client := redis.NewClient(options)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis connect failed: %w", err)
	}

	logger.Info("redis client connected", "addr", redacted)
	return client, nil
}
