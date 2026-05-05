CREATE TABLE IF NOT EXISTS telemetry_ingest_dedupe (
    idempotency_key TEXT PRIMARY KEY,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);