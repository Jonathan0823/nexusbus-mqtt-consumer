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

// NewRepository creates a new PostgreSQL repository from an existing pool.
func NewRepository(pool *pgxpool.Pool, logger *logging.Logger) *Repository {
	return &Repository{
		pool:   pool,
		logger: logger,
	}
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
		if row.IdempotencyKey == "" {
			return inserted, fmt.Errorf("idempotency key required")
		}

		// Try to insert idempotency key first
		dedupeKey := row.IdempotencyKey
		result, err := tx.Exec(ctx,
			"INSERT INTO telemetry_ingest_dedupe (idempotency_key, first_seen_at) VALUES ($1, NOW()) ON CONFLICT (idempotency_key) DO NOTHING",
			dedupeKey,
		)

		if err != nil {
			return inserted, fmt.Errorf("insert dedupe key: %w", err)
		}

		// Duplicate - skip this row if the key already existed.
		if result.RowsAffected() == 0 {
			continue
		}

		// Insert the telemetry row
		metricsJSON, err := json.Marshal(row.Metrics)
		if err != nil {
			return inserted, fmt.Errorf("marshal metrics: %w", err)
		}

		var rawPayloadJSON []byte
		if row.RawPayload != nil {
			rawPayloadJSON, err = json.Marshal(*row.RawPayload)
			if err != nil {
				return inserted, fmt.Errorf("marshal raw payload: %w", err)
			}
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
