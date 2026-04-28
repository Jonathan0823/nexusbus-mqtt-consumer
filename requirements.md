# Telemetry Ingestion Pipeline — Product Requirements Document

**System:** Telemetry Ingestion Pipeline, Enriched JSONB
**Version:** v1.0 Draft
**Target Runtime:** Single Go binary
**Delivery Model:** At-least-once delivery with idempotent database writes

---

## 1. Objective

Build a reliable single-binary Go service that ingests raw Modbus array telemetry from Edge Gateways through MQTT, buffers messages durably using Redis Streams, transforms raw register arrays into semantic metrics using a static Device Profile Registry, and asynchronously persists enriched telemetry into PostgreSQL or TimescaleDB using JSONB.

The system is intended for long-running industrial telemetry use where process restarts, PostgreSQL downtime, duplicate deliveries, and unknown device profiles must be handled safely.

---

## 2. Background

Edge Gateways collect Modbus data from field devices and publish JSON payloads through MQTT. These payloads contain raw integer arrays rather than semantic metric names.

The backend service enriches these raw payloads into meaningful metrics such as voltage, current, frequency, temperature, state of charge, or other device-specific measurements.

Because telemetry data may be used for monitoring, historical analytics, troubleshooting, and customer reporting, the ingestion pipeline must be resilient and auditable.

---

## 3. Goals

The system shall:

1. Ingest raw telemetry payloads from MQTT.
2. Validate mandatory telemetry fields.
3. Persist accepted raw messages into Redis Streams before processing.
4. Decouple ingestion from database persistence.
5. Transform raw Modbus register arrays using a static Device Profile Registry.
6. Store enriched metrics in PostgreSQL or TimescaleDB using JSONB.
7. Prevent duplicate rows during retry or crash recovery.
8. Recover unacknowledged Redis messages after worker crash or service restart.
9. Continue accepting telemetry during PostgreSQL downtime as long as Redis is available.
10. Provide health, readiness, and operational metrics for production monitoring.

---

## 4. Non-Goals / Out of Scope

The system shall not:

1. Perform Modbus polling directly.
2. Control edge devices.
3. Validate business anomalies such as alarm thresholds, over-voltage, under-voltage, over-temperature, or abnormal current.
4. Perform dynamic database schema migration for every new metric.
5. Replace an alarm/event engine.
6. Replace dashboard or analytics applications.
7. Perform device provisioning or device lifecycle management.
8. Guarantee true distributed exactly-once delivery.

The system provides:

```text
At-least-once delivery + idempotent database writes
```

---

## 5. System Boundaries

## 5.1 Input Boundary

Input source:

```text
Edge Gateway → MQTT Broker → Telemetry Ingestion Service
```

The MQTT payload is JSON containing raw Modbus register values.

## 5.2 Output Boundary

Output target:

```text
Telemetry Ingestion Service → PostgreSQL / TimescaleDB
```

## 5.3 Internal Durable Buffer

Internal flow:

```text
MQTT Ingestor → Redis Stream → Worker → PostgreSQL / TimescaleDB
```

Redis Streams acts as the durable decoupling layer between ingestion and persistence.

---

## 6. Core Reliability Principle

The Go service must be treated as stateless and replaceable.

The Go process may crash or restart at any time. Telemetry durability must not depend on Go memory.

Durability must be handled by:

1. Edge Gateway local buffer, where available.
2. MQTT QoS 1 and persistent broker session.
3. Redis Streams with persistence enabled.
4. PostgreSQL transaction commit.
5. Deterministic idempotency keys for duplicate prevention.

---

## 7. Delivery Semantics

The pipeline uses:

```text
At-least-once delivery + idempotent database writes
```

This means:

1. A message may be delivered more than once.
2. A message may be processed more than once after retry or crash recovery.
3. The database must store the telemetry item only once.
4. Duplicate prevention is handled through deterministic `message_id` or `idempotency_key`.

---

## 8. Functional Requirements

## FR-001 — MQTT Subscription

The service shall subscribe to configured MQTT topics.

Recommended topic:

```text
telemetry/raw/#
```

Recommended MQTT settings:

```text
QoS: 1
Client ID: fixed/stable
Clean Session: false for MQTT v3.1.1
Session Expiry: non-zero/long duration for MQTT v5
```

---

## FR-002 — Minimal Payload Validation

The Ingestor shall validate mandatory fields before writing the payload to Redis.

Mandatory fields:

```text
device_id
values
```

Recommended fields:

```text
timestamp
register_type
address
count
source
profile_id
message_id
```

Reject the message if:

1. `device_id` is missing or empty.
2. `values` is missing.
3. `values` is not an array.
4. Payload is not valid JSON.

---

## FR-003 — Redis Stream Buffering

Valid MQTT messages shall be written to Redis Stream using `XADD`.

Default stream:

```text
telemetry_stream
```

The Ingestor must not perform semantic transformation.

---

## FR-004 — Strict Decoupling

The Ingestor and Worker shall communicate only through Redis I/O.

The Ingestor shall not call PostgreSQL.

The Worker shall not depend on in-memory messages from the Ingestor.

---

## FR-005 — Device Profile Registry

The service shall load a static Device Profile Registry from configuration on startup.

Default file:

```text
profiles.yaml
```

Mapping from raw array index to semantic metric shall not be hardcoded in Go business logic.

---

## FR-006 — Profile Matching

The Worker shall identify the appropriate profile using priority order:

1. Explicit `profile_id` from payload.
2. Exact `device_id` mapping.
3. Device ID prefix or regex rule.
4. `register_type` + `address` + `count`.
5. Fallback `unknown` profile.

The system should not rely only on `count`.

---

## FR-007 — Enrichment Engine

The Worker shall convert raw `values` array into a semantic JSON object using the selected profile.

Example raw values:

```json
{
  "device_id": "office-eng",
  "timestamp": "2026-04-27T10:15:00.000+07:00",
  "register_type": "input",
  "address": 0,
  "count": 9,
  "values": [2275, 625, 0, 937, 0, 29305, 4, 500, 66],
  "source": "live"
}
```

Example metrics:

```json
{
  "voltage_r": 22.75,
  "current_r": 6.25,
  "frequency": 50.0,
  "unknown_metric": 66
}
```

---

## FR-008 — Timestamp Handling

The `time` column shall represent original measurement timestamp from the MQTT/Edge payload.

If a valid timestamp is not provided, PostgreSQL `DEFAULT NOW()` may be used as fallback.

The system shall also store `received_at`.

Meaning:

```text
time        = measurement time from Edge/MQTT
received_at = backend receive/insert time
```

---

## FR-009 — Idempotency

The Worker shall generate or consume a deterministic idempotency key for each telemetry item.

Preferred source:

```text
message_id from Edge Gateway
```

Fallback:

```text
SHA256(device_id + timestamp_utc + profile_id)
```

or:

```text
SHA256(device_id + timestamp_utc + register_type + address + count)
```

The same telemetry item must always produce the same key.

---

## FR-010 — Duplicate Prevention

The database write flow shall prevent duplicate telemetry rows during retries or crash recovery.

Recommended approach:

```text
telemetry_ingest_dedupe table
```

The Worker inserts the idempotency key first. If the key already exists, the message is treated as already processed.

---

## FR-011 — PostgreSQL Persistence

The Worker shall persist enriched telemetry into PostgreSQL or TimescaleDB.

The main table shall store:

1. Measurement timestamp.
2. Backend received timestamp.
3. Device ID.
4. Profile ID.
5. Register metadata.
6. Enriched metrics as JSONB.
7. Source.
8. Idempotency key.
9. Optional raw payload.

---

## FR-012 — Batch Insert

The Worker shall support batch inserts controlled by:

```text
batch_size
batch_timeout
```

Recommended default:

```text
batch_size: 500
batch_timeout: 1s
```

---

## FR-013 — Redis Acknowledgement

The Worker shall execute `XACK` only after PostgreSQL transaction commit succeeds.

If commit fails, the Redis message shall remain unacknowledged.

---

## FR-014 — Pending Message Recovery

The Worker shall recover pending Redis messages using:

```text
XPENDING / XAUTOCLAIM
```

or equivalent Redis Streams recovery mechanism.

---

## FR-015 — Deadletter Handling

Messages that fail permanently or exceed maximum retry count shall be moved to:

```text
telemetry_deadletter_stream
```

Deadletter payload shall include:

1. Original payload.
2. Error reason.
3. Failure timestamp.
4. Retry count.
5. Redis Stream ID.

---

## FR-016 — Unknown Profile Handling

If no matching profile is found, the Worker shall not drop the message.

It shall store:

```json
{
  "raw_values": [2275, 625, 0, 937, 0, 29305, 4, 500, 66]
}
```

The `profile_id` shall be:

```text
unknown
```

---

## FR-017 — Observability

The service shall expose:

```text
/healthz
/readyz
/metrics
```

---

## FR-018 — Graceful Shutdown

On shutdown signal, the service shall:

1. Stop accepting new MQTT messages.
2. Stop reading new Redis messages.
3. Finish current PostgreSQL transaction if possible.
4. Acknowledge Redis messages only after successful commit.
5. Close resources cleanly.

---

## 9. Non-Functional Requirements

## NFR-001 — Reliability

The system must be resilient to:

1. PostgreSQL downtime.
2. Worker crash.
3. Service restart.
4. Duplicate MQTT delivery.
5. Redis pending messages.
6. Unknown profiles.
7. Invalid payloads.

## NFR-002 — Durability

Redis shall use persistence.

Recommended:

```conf
appendonly yes
appendfsync everysec
```

## NFR-003 — MQTT Broker Persistence

Mosquitto or the selected MQTT broker should enable persistence.

Recommended:

```conf
persistence true
persistence_location /var/lib/mosquitto/
autosave_interval 5
```

## NFR-004 — Performance

The MQTT callback shall stay lightweight.

The Worker shall use batch database writes.

## NFR-005 — Scalability

The same binary should be runnable as multiple instances.

MQTT shared subscription may be used:

```text
$share/telemetry_ingestors/telemetry/raw/#
```

Redis consumer group:

```text
telemetry_workers
```

## NFR-006 — Maintainability

Device-specific mapping must be in `profiles.yaml`, not hardcoded in Go.

## NFR-007 — Security

The system should support:

1. MQTT authentication.
2. MQTT TLS where required.
3. Redis authentication.
4. PostgreSQL TLS where required.
5. Secret configuration through environment variables or secret files.

## NFR-008 — Operability

The service shall be compatible with:

1. systemd restart policy.
2. Container restart policy.
3. Log collection.
4. Metrics scraping.
5. Health checks.

---

## 10. Acceptance Criteria

The system is accepted when:

1. Valid MQTT payload is written to Redis Stream.
2. Worker reads Redis message using consumer group.
3. Worker transforms raw values using `profiles.yaml`.
4. Worker inserts enriched JSONB into PostgreSQL.
5. Redis `XACK` occurs only after PostgreSQL commit.
6. PostgreSQL downtime does not lose accepted Redis messages.
7. Worker crash before commit results in message retry.
8. Worker crash after commit but before `XACK` does not create duplicate rows.
9. Unknown profile is stored as `raw_values`, not dropped.
10. Invalid payload is rejected or deadlettered.
11. Pending Redis messages are recovered.
12. `/healthz`, `/readyz`, and `/metrics` are available.
13. Service restarts cleanly using supervisor or container restart policy.
