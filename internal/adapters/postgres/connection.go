package postgres

import (
	"context"

	"modbus-mqtt-consumer/internal/platform/logging"
)

// NewConnection creates a PostgreSQL repository connection.
// This factory follows the design.md adapter creation pattern.
func NewConnection(ctx context.Context, cfg Config, logger *logging.Logger) (*Repository, error) {
	return NewRepository(ctx, cfg, logger)
}
