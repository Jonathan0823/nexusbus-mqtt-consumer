CREATE INDEX IF NOT EXISTS ix_telemetry_device_time
    ON telemetry_enriched (device_id, time DESC);

CREATE INDEX IF NOT EXISTS ix_telemetry_profile_time
    ON telemetry_enriched (profile_id, time DESC);

CREATE INDEX IF NOT EXISTS ix_telemetry_metrics
    ON telemetry_enriched USING GIN (metrics);

CREATE INDEX IF NOT EXISTS ix_telemetry_idempotency
    ON telemetry_enriched (idempotency_key);