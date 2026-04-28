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
