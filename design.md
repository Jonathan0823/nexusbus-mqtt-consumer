# Telemetry Ingestion Pipeline — Technical Design

**System:** Telemetry Ingestion Pipeline, Enriched JSONB
**Version:** v1.1 Draft
**Target Runtime:** Single Go binary
**Architecture Style:** Hexagonal Architecture / Ports & Adapters with Clean Architecture dependency rules
**Delivery Model:** At-least-once delivery with idempotent database writes

---

## 1. Architecture Style

The service uses a **Hexagonal Architecture / Ports & Adapters** style with **Clean Architecture dependency rules**.

The system is integration-heavy rather than business-domain-heavy. The main complexity is reliable movement of data across MQTT, Redis Streams, PostgreSQL, and HTTP APIs.

Therefore, the design intentionally avoids full Domain-Driven Design complexity. DDD naming is used lightly for clarity, but the project does not require heavy aggregate roots, domain events, bounded contexts, or deep domain service layers.

Recommended architecture principle:

```text
core      = what the system does
adapters  = how the system talks to external systems
platform  = how the application is configured, run, observed, and stopped
cmd       = wiring / composition root
```

---

## 2. Package Structure

Recommended Go project structure:

```text
/cmd
  /telemetryd
    main.go

/internal
  /core
    /domain
      telemetry.go
      profile.go
      metric.go
      idempotency.go
      error.go

    /ports
      message_buffer.go
      telemetry_repository.go
      profile_registry.go
      mqtt_subscriber.go
      metrics_recorder.go
      clock.go

    /service
      ingest_service.go
      worker_service.go
      query_service.go
      profile_service.go

  /adapters
    /mqtt
      subscriber.go
      config.go

    /redis
      stream_buffer.go
      retry_tracker.go

    /postgres
      telemetry_repository.go
      schema.go
      migration.go

    /http
      router.go
      api_handler.go
      health_handler.go
      metrics_handler.go

    /yamlprofile
      registry.go
      parser.go

  /platform
    /config
      config.go
      env.go
      validation.go

    /logging
      logger.go

    /metrics
      prometheus.go
      recorder.go

    /shutdown
      shutdown.go
      signal.go

    /runtime
      role.go
      runner.go
```

---

## 3. Layer Responsibilities

## 3.1 `/internal/core/domain`

Contains pure domain/application data structures and small deterministic logic.

Examples:

```text
RawTelemetryPayload
RawTelemetryMessage
BufferedMessage
DeviceProfile
MetricMapping
EnrichedTelemetry
IdempotencyKey
DeadletterMessage
TelemetryRangeQuery
```

Allowed responsibilities:

```text
- Normalize timestamp
- Represent telemetry payloads
- Represent enriched telemetry rows
- Calculate simple metric values
- Build deterministic idempotency key
- Define domain-level validation errors
```

Rules:

```text
- Must not import Redis, MQTT, PostgreSQL, HTTP, or Prometheus libraries.
- Should depend only on the Go standard library where possible.
```

---

## 3.2 `/internal/core/ports`

Contains interfaces used by the core services.

Ports define what the application needs from the outside world without depending on concrete technologies.

Example ports:

```go
type MessageBuffer interface {
    Add(ctx context.Context, msg domain.RawTelemetryMessage) error
    ReadBatch(ctx context.Context, max int) ([]domain.BufferedMessage, error)
    Ack(ctx context.Context, ids []string) error
    ClaimStale(ctx context.Context, minIdle time.Duration, max int) ([]domain.BufferedMessage, error)
    Deadletter(ctx context.Context, msg domain.BufferedMessage, reason string) error
}
```

```go
type TelemetryRepository interface {
    InsertBatchIdempotent(ctx context.Context, rows []domain.EnrichedTelemetry) error
    GetLatest(ctx context.Context, deviceID string) (*domain.EnrichedTelemetry, error)
    QueryRange(ctx context.Context, q domain.TelemetryRangeQuery) ([]domain.EnrichedTelemetry, error)
}
```

```go
type ProfileRegistry interface {
    Match(payload domain.RawTelemetryPayload) (*domain.DeviceProfile, error)
    Transform(payload domain.RawTelemetryPayload, profile domain.DeviceProfile) (map[string]any, error)
}
```

```go
type MetricsRecorder interface {
    IncMQTTReceived()
    IncMQTTRejected(reason string)
    IncRedisXAddError()
    IncWorkerProcessed()
    IncWorkerDuplicate()
    IncWorkerFailed(reason string)
    IncDeadlettered()
    ObserveBatchInsertDuration(seconds float64)
}
```

Rules:

```text
- Ports belong to core, not adapters.
- Adapters implement ports.
- Services depend on ports.
```

---

## 3.3 `/internal/core/service`

Contains application services / use cases.

Main services:

```text
IngestService
WorkerService
QueryService
ProfileService
```

### IngestService

Responsibilities:

```text
- Receive raw MQTT payload from adapter
- Validate mandatory fields
- Add backend received timestamp
- Write accepted message to MessageBuffer
- Return success/failure to MQTT adapter
```

It does not know that the message buffer is Redis.

### WorkerService

Responsibilities:

```text
- Read messages from MessageBuffer
- Claim stale pending messages
- Parse raw payload
- Match device profile
- Transform raw values into metrics
- Build idempotency key
- Insert enriched telemetry using TelemetryRepository
- Ack only after successful insert/duplicate handling
- Deadletter invalid or repeatedly failing messages
```

It does not know that the buffer is Redis or that the database is PostgreSQL.

### QueryService

Responsibilities:

```text
- Serve latest telemetry query
- Serve telemetry range query
- Serve profile/device query if needed
- Enforce query limit and time range guardrails
```

It does not know that the HTTP adapter is REST.

---

## 3.4 `/internal/adapters`

Adapters connect the core application to external systems.

Adapter examples:

```text
/adapters/mqtt      implements MQTT subscriber behavior
/adapters/redis     implements MessageBuffer using Redis Streams
/adapters/postgres  implements TelemetryRepository using pgx
/adapters/http      exposes API, health, readiness, and metrics endpoints
/adapters/yamlprofile implements ProfileRegistry using profiles.yaml
```

Rules:

```text
- Adapters may import external libraries.
- Adapters may import core/domain and core/ports.
- Adapters should not contain core business flow.
```

---

## 3.5 `/internal/platform`

Platform contains cross-cutting runtime infrastructure.

Examples:

```text
config
logging
metrics
shutdown
runtime role
```

Platform is not core business logic and is not a domain adapter like MQTT/Redis/PostgreSQL.

Recommended use:

```text
/platform/config   = load YAML/env config
/platform/logging  = initialize slog/zap/logger
/platform/metrics  = Prometheus implementation of MetricsRecorder
/platform/shutdown = signal handling and graceful shutdown
/platform/runtime  = role=all / role=ingestor / role=worker / role=api runner
```

Rules:

```text
- core/domain should not import platform.
- core/service should not depend on platform concrete implementation.
- platform can wire concrete metrics/logging/shutdown implementations in cmd/main.go.
```

---

## 4. Dependency Rule

Allowed dependency direction:

```text
cmd → platform
cmd → adapters
cmd → core/service

adapters → core/ports
adapters → core/domain

core/service → core/ports
core/service → core/domain

core/domain → standard library only
```

Disallowed dependencies:

```text
core/domain → adapters
core/domain → platform
core/domain → Redis/MQTT/PostgreSQL/HTTP libraries
core/service → Redis client directly
core/service → pgx directly
core/service → MQTT client directly
adapters → platform business behavior
```

Composition root:

```text
/cmd/telemetryd/main.go
```

`main.go` is responsible for wiring concrete adapters into core services.

---

## 5. Runtime Components

The binary may run one or more runtime components depending on role.

```text
role=all
  - MQTT Ingestor
  - Redis Worker
  - Read-only HTTP API
  - Health / readiness / metrics server

role=ingestor
  - MQTT Ingestor only

role=worker
  - Redis Worker only

role=api
  - Read-only HTTP API
  - Health / readiness / metrics server
```

Recommended initial deployment:

```text
telemetry-service --role=all
```

Recommended scalable deployment later:

```text
telemetry-service --role=ingestor
telemetry-service --role=worker
telemetry-service --role=api
```

This keeps one binary artifact while allowing runtime separation.

---

## 6. High-Level Runtime Flow

```text
Edge Gateway
   ↓ MQTT QoS 1
MQTT Broker / Mosquitto
   ↓
/adapters/mqtt
   ↓ calls
/core/service.IngestService
   ↓ uses MessageBuffer port
/adapters/redis
   ↓ XADD
Redis Stream
   ↓ XREADGROUP / XAUTOCLAIM
/adapters/redis
   ↓ returns BufferedMessage
/core/service.WorkerService
   ↓ uses ProfileRegistry + TelemetryRepository ports
/adapters/yamlprofile + /adapters/postgres
   ↓
PostgreSQL / TimescaleDB / Plain PostgreSQL
```

---

## 7. MQTT Design

Recommended topic:

```text
telemetry/raw/#
```

Recommended settings:

```yaml
mqtt:
  broker: "tcp://localhost:1883"
  client_id: "telemetry-ingestion-service"
  topic: "telemetry/raw/#"
  qos: 1
  clean_session: false
  session_expiry: "7d"
```

Recommended acknowledgement flow:

```text
Receive MQTT message
Validate minimal payload
XADD to Redis through IngestService
ACK MQTT message only after successful XADD
```

If the MQTT library auto-acknowledges before Redis `XADD`, there is a small failure window where the process may crash after MQTT receive but before Redis write.

---

## 8. MQTT Payload Contract

Minimum payload:

```json
{
  "device_id": "office-eng",
  "values": [2275, 625, 0, 937, 0, 29305, 4, 500, 66]
}
```

Recommended payload:

```json
{
  "device_id": "office-eng",
  "timestamp": "2026-04-27T10:15:00.000+07:00",
  "message_id": "office-eng|2026-04-27T03:15:00.000Z|power_meter_9",
  "profile_id": "power_meter_9",
  "register_type": "input",
  "address": 0,
  "count": 9,
  "values": [2275, 625, 0, 937, 0, 29305, 4, 500, 66],
  "source": "live"
}
```

Mandatory fields:

```text
device_id
values
```

Recommended fields:

```text
timestamp
message_id
profile_id
register_type
address
count
source
```

---

## 9. Redis Stream Design

Primary stream:

```text
telemetry_stream
```

Deadletter stream:

```text
telemetry_deadletter_stream
```

Consumer group:

```text
telemetry_workers
```

New message read:

```text
XREADGROUP GROUP telemetry_workers consumer-1 COUNT 500 BLOCK 1000 STREAMS telemetry_stream >
```

Pending recovery:

```text
XAUTOCLAIM telemetry_stream telemetry_workers consumer-1 60000 0-0 COUNT 500
```

Acknowledgement rule:

```text
XACK only after PostgreSQL transaction commit succeeds, or after duplicate is confirmed through idempotency table.
```

---

## 10. Device Profile Registry Design

Default profile file:

```text
profiles.yaml
```

Example profile:

```yaml
profiles:
  power_meter_9:
    match:
      register_type: "input"
      address: 0
      count: 9
      device_id_prefix: "office-"

    mapping:
      - index: 0
        key: "voltage_r"
        type: "uint16"
        multiplier: 0.01
        unit: "V"

      - index: 1
        key: "current_r"
        type: "uint16"
        multiplier: 0.01
        unit: "A"

      - index: 7
        key: "frequency"
        type: "uint16"
        multiplier: 0.1
        unit: "Hz"
```

Profile matching priority:

```text
1. Explicit profile_id
2. Exact device_id mapping
3. device_id prefix / regex
4. register_type + address + count
5. unknown profile fallback
```

---

## 11. Idempotency Design

Preferred idempotency source:

```text
message_id from Edge Gateway
```

Recommended format:

```text
{device_id}|{timestamp_utc}|{profile_id}
```

Example:

```text
office-eng|2026-04-27T03:15:00.000Z|power_meter_9
```

Backend fallback if `message_id` is missing:

```text
SHA256(device_id + "|" + timestamp_utc + "|" + profile_id)
```

If `profile_id` is missing:

```text
SHA256(device_id + "|" + timestamp_utc + "|" + register_type + "|" + address + "|" + count)
```

If both timestamp and message ID are missing, strong deduplication cannot be guaranteed.

---

## 12. Database Design

Preferred database:

```text
PostgreSQL + TimescaleDB
```

Fallback database:

```text
Plain PostgreSQL with native range partitioning by time
```

Early test option:

```text
Plain PostgreSQL single table
```

---

## 12.1 Main Telemetry Table

```sql
CREATE TABLE telemetry_enriched (
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
```

For TimescaleDB:

```sql
SELECT create_hypertable('telemetry_enriched', 'time');
```

For plain PostgreSQL production fallback:

```sql
CREATE TABLE telemetry_enriched (
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
) PARTITION BY RANGE (time);
```

---

## 12.2 Deduplication Table

Use a regular PostgreSQL table for global deduplication:

```sql
CREATE TABLE telemetry_ingest_dedupe (
    idempotency_key TEXT PRIMARY KEY,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## 12.3 Recommended Indexes

```sql
CREATE INDEX ix_telemetry_device_time
ON telemetry_enriched (device_id, time DESC);

CREATE INDEX ix_telemetry_profile_time
ON telemetry_enriched (profile_id, time DESC);

CREATE INDEX ix_telemetry_metrics
ON telemetry_enriched USING GIN (metrics);

CREATE INDEX ix_telemetry_idempotency
ON telemetry_enriched (idempotency_key);
```

For plain PostgreSQL large time-series data:

```sql
CREATE INDEX ix_telemetry_time_brin
ON telemetry_enriched USING BRIN (time);
```

---

## 13. Worker Processing Flow

Normal worker flow:

```text
1. Read messages using MessageBuffer.ReadBatch.
2. Parse raw JSON payload.
3. Validate worker-level fields.
4. Match profile using ProfileRegistry.
5. Transform values array into metrics object.
6. Build idempotency key.
7. Add enriched telemetry to batch.
8. Insert batch using TelemetryRepository.InsertBatchIdempotent.
9. Ack Redis messages only after successful insert or confirmed duplicate.
```

Pending recovery flow:

```text
1. Claim stale messages using MessageBuffer.ClaimStale.
2. Reprocess claimed messages.
3. Generate the same idempotency key.
4. Insert with dedupe protection.
5. Ack after successful commit or confirmed duplicate.
```

Unknown profile flow:

```text
profile_id = "unknown"
metrics = {"raw_values": [...]}
```

---

## 14. Read API Design

The same binary may expose a read-only HTTP API if query use is controlled.

Recommended internal isolation:

```text
Worker / Writer → PostgreSQL write pool
HTTP API        → PostgreSQL read pool
```

Suggested endpoints:

```text
GET /api/v1/devices
GET /api/v1/devices/{device_id}/latest
GET /api/v1/devices/{device_id}/telemetry?from=...&to=...&limit=...
GET /api/v1/devices/{device_id}/summary?from=...&to=...
GET /api/v1/profiles
```

API guardrails:

```text
- Read-only queries
- Query timeout
- Maximum time range
- Pagination or row limit
- No unbounded queries
- Summary tables or materialized views for dashboard analytics
```

---

## 15. Failure Recovery Design

## 15.1 PostgreSQL Down

```text
Worker cannot commit inserts.
Redis messages remain pending or unread.
XACK is not executed.
Messages accumulate in Redis.
Worker retries PostgreSQL with exponential backoff.
```

## 15.2 Binary Dies Before MQTT Receive

```text
MQTT broker queues messages if persistent session is enabled.
```

## 15.3 Binary Dies After MQTT Receive but Before Redis XADD

Critical failure window.

Best mitigation:

```text
Manual MQTT ACK after Redis XADD succeeds.
```

If manual ACK is not supported:

```text
Edge Gateway local buffer + retry with deterministic message_id.
```

## 15.4 Binary Dies After Redis XADD but Before Processing

```text
Message is safe in Redis.
```

## 15.5 Binary Dies After XREADGROUP but Before PostgreSQL Commit

```text
Message remains pending in Redis.
Worker recovers it using XAUTOCLAIM.
```

## 15.6 Binary Dies After PostgreSQL Commit but Before Redis XACK

```text
Database row exists.
Redis message remains pending.
Worker reprocesses message.
Same idempotency_key is generated.
Dedupe table rejects duplicate.
Worker XACKs Redis message.
```

---

## 16. Deadletter Design

Deadletter stream:

```text
telemetry_deadletter_stream
```

Recommended max retry:

```yaml
worker:
  max_retries: 5
```

Deadletter payload example:

```json
{
  "redis_stream_id": "1714190100000-0",
  "failed_at": "2026-04-27T03:20:00.000Z",
  "retry_count": 5,
  "error": "invalid values array: expected []int",
  "original_payload": {
    "device_id": "office-eng",
    "values": [2275, null, "abc"]
  }
}
```

---

## 17. Observability Integration

Core services should record metrics through a `MetricsRecorder` port.

Prometheus-specific implementation lives in:

```text
/internal/platform/metrics
```

HTTP handlers for `/metrics` live in:

```text
/internal/adapters/http
```

Required endpoints:

```text
GET /healthz
GET /readyz
GET /metrics
```

---

## 18. Final Technical Position

The system should be implemented as:

```text
Single Go binary using Hexagonal Architecture / Ports & Adapters.
```

Final structure:

```text
core      = domain, ports, and services
adapters  = MQTT, Redis, PostgreSQL, HTTP, YAML profile loader
platform  = config, logging, metrics, shutdown, runtime role
cmd       = wiring / composition root
```

This keeps the system clean, testable, and reliable without overengineering it with full DDD.
