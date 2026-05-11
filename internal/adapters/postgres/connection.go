package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// NewPool creates a PostgreSQL connection pool.
func NewPool(ctx context.Context, cfg config.PostgresConfig, logger *logging.Logger) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	poolConfig.MaxConns = int32(cfg.MaxWriteConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping failed: %w", err)
	}

	dbName := cfg.DSN
	for i := len(dbName) - 1; i >= 0; i-- {
		if dbName[i] == '/' {
			dbName = dbName[i+1:]
			if q := strings.IndexByte(dbName, '?'); q >= 0 {
				dbName = dbName[:q]
			}
			break
		}
	}
	logger.Info("postgres connected", "max_conns", cfg.MaxWriteConns, "db", dbName)
	return pool, nil
}
