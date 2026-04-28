package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// Repository implements TelemetryRepository using pgx.
type Repository struct {
	pool   *pgxpool.Pool
	logger *logging.Logger
}

// Config holds PostgreSQL connection configuration.
type Config struct {
	DSN           string
	MaxWriteConns int
	MaxReadConns  int
}

// NewRepository creates a new PostgreSQL repository.
func NewRepository(ctx context.Context, cfg Config, logger *logging.Logger) (*Repository, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	poolConfig.MaxConns = int32(cfg.MaxWriteConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping failed: %w", err)
	}

	logger.Info("postgres connected", "max_conns", cfg.MaxWriteConns)

	return &Repository{
		pool:   pool,
		logger: logger,
	}, nil
}

// EnsureSchema creates the required tables if they don't exist.
func (r *Repository) EnsureSchema(ctx context.Context) error {
	// Main telemetry table
	schema := `
		CREATE TABLE IF NOT EXISTS telemetry_enriched (
			time             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			device_id        VARCHAR(80) NOT NULL,
			profile_id       VARCHAR(80) NOT NULL,
			register_type    VARCHAR(20),
			address          INTEGER,
			count            INTEGER,
			source           VARCHAR(20),
			idempotency_key  TEXT NOT NULL,
			metrics          JSONB NOT NULL,
			raw_payload      JSONB,
			inserted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		
		CREATE TABLE IF NOT EXISTS telemetry_ingest_dedupe (
			idempotency_key TEXT PRIMARY KEY,
			first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		
		CREATE INDEX IF NOT EXISTS ix_telemetry_device_time 
			ON telemetry_enriched (device_id, time DESC);
		
		CREATE INDEX IF NOT EXISTS ix_telemetry_profile_time 
			ON telemetry_enriched (profile_id, time DESC);
		
		CREATE INDEX IF NOT EXISTS ix_telemetry_metrics 
			ON telemetry_enriched USING GIN (metrics);
		
		CREATE INDEX IF NOT EXISTS ix_telemetry_idempotency 
			ON telemetry_enriched (idempotency_key);
	`

	_, err := r.pool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	r.logger.Info("postgres schema ensured")
	return nil
}

// InsertBatchIdempotent inserts a batch of telemetry rows idempotently.
func (r *Repository) InsertBatchIdempotent(ctx context.Context, rows []domain.EnrichedTelemetry) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// Use a transaction for atomicity
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	inserted := 0

	for _, row := range rows {
		// Try to insert idempotency key first
		dedupeKey := row.IdempotencyKey
		_, err := tx.Exec(ctx,
			"INSERT INTO telemetry_ingest_dedupe (idempotency_key, first_seen_at) VALUES ($1, NOW()) ON CONFLICT (idempotency_key) DO NOTHING",
			dedupeKey,
		)

		if err != nil {
			return inserted, fmt.Errorf("insert dedupe key: %w", err)
		}

		// Check if it was a duplicate
		var exists bool
		err = tx.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM telemetry_ingest_dedupe WHERE idempotency_key = $1)",
			dedupeKey,
		).Scan(&exists)

		if err != nil {
			return inserted, fmt.Errorf("check dedupe: %w", err)
		}

		if !exists {
			// Duplicate - skip this row
			continue
		}

		// Insert the telemetry row
		metricsJSON, err := json.Marshal(row.Metrics)
		if err != nil {
			return inserted, fmt.Errorf("marshal metrics: %w", err)
		}

		var rawPayloadJSON []byte
		if row.RawPayload != nil {
			rawPayloadJSON, _ = json.Marshal(*row.RawPayload)
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO telemetry_enriched 
				(time, received_at, device_id, profile_id, register_type, address, count, source, idempotency_key, metrics, raw_payload)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			row.Time, row.ReceivedAt, row.DeviceID, row.ProfileID,
			row.RegisterType, row.Address, row.Count, row.Source,
			row.IdempotencyKey, metricsJSON, rawPayloadJSON,
		)

		if err != nil {
			return inserted, fmt.Errorf("insert telemetry: %w", err)
		}

		inserted++
	}

	if err := tx.Commit(ctx); err != nil {
		return inserted, fmt.Errorf("commit tx: %w", err)
	}

	r.logger.Debug("postgres batch insert", "inserted", inserted, "total", len(rows))
	return inserted, nil
}

// GetLatest returns the most recent telemetry for a device.
func (r *Repository) GetLatest(ctx context.Context, deviceID string) (*domain.EnrichedTelemetry, error) {
	var row domain.EnrichedTelemetry
	var metricsJSON, rawPayloadJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT time, received_at, device_id, profile_id, register_type, address, count, source, idempotency_key, metrics, raw_payload
		FROM telemetry_enriched WHERE device_id = $1 ORDER BY time DESC LIMIT 1`,
		deviceID,
	).Scan(
		&row.Time, &row.ReceivedAt, &row.DeviceID, &row.ProfileID,
		&row.RegisterType, &row.Address, &row.Count, &row.Source,
		&row.IdempotencyKey, &metricsJSON, &rawPayloadJSON,
	)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get latest: %w", err)
	}

	json.Unmarshal(metricsJSON, &row.Metrics)
	if rawPayloadJSON != nil {
		raw := json.RawMessage(rawPayloadJSON)
		row.RawPayload = &raw
	}

	return &row, nil
}

// QueryRange returns telemetry within a time range.
func (r *Repository) QueryRange(ctx context.Context, q domain.TelemetryRangeQuery) ([]domain.EnrichedTelemetry, error) {
	query := `
		SELECT time, received_at, device_id, profile_id, register_type, address, count, source, idempotency_key, metrics, raw_payload
		FROM telemetry_enriched 
		WHERE device_id = $1 AND time >= $2 AND time <= $3
		ORDER BY time DESC
		LIMIT $4
	`

	rows, err := r.pool.Query(ctx, query, q.DeviceID, q.From, q.To, q.Limit)
	if err != nil {
		return nil, fmt.Errorf("query range: %w", err)
	}
	defer rows.Close()

	var results []domain.EnrichedTelemetry
	for rows.Next() {
		var row domain.EnrichedTelemetry
		var metricsJSON, rawPayloadJSON []byte

		err := rows.Scan(
			&row.Time, &row.ReceivedAt, &row.DeviceID, &row.ProfileID,
			&row.RegisterType, &row.Address, &row.Count, &row.Source,
			&row.IdempotencyKey, &metricsJSON, &rawPayloadJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		json.Unmarshal(metricsJSON, &row.Metrics)
		if rawPayloadJSON != nil {
			raw := json.RawMessage(rawPayloadJSON)
			row.RawPayload = &raw
		}

		results = append(results, row)
	}

	return results, rows.Err()
}

// Ping checks database connectivity.
func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// Close closes the connection pool.
func (r *Repository) Close() {
	r.pool.Close()
}
