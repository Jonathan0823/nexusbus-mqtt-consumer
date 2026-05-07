# PR Plan: Add Role-Based Deployment Modes

## Related issue
- No related issue (standalone feature)

## Summary
Adds `SERVICE_ROLE` environment variable to control which components initialize, enabling role-based deployment modes in a single binary:

- **all** (default): Full stack — HTTP + MQTT + Redis + PostgreSQL + Worker
- **http-only**: HTTP API + PostgreSQL only (for scaling read endpoints independently)
- **ingest-only**: (placeholder) MQTT + Redis only
- **worker-only**: (placeholder) Worker + Redis only

Changes:
- Added `ServiceRole` type and constants to `internal/platform/config`
- Added `setRole()` helper to parse `SERVICE_ROLE` env var
- Extended `Wiring` struct with `Role` field
- Added `buildHTTPOnly()` builder for http-only role
- Updated `NewWiring()` to dispatch to role-specific builders
- Made `main.go` role-aware with conditional startup and nil checks
- Updated `.env.example` with `SERVICE_ROLE` documentation

## Scope
- In scope:
  - Config: ServiceRole type, setRole() helper, Role field in ServiceConfig
  - Wiring: buildHTTPOnly() builder, role dispatch in NewWiring()
  - Main: role-based conditional startup logic
  - Docs: .env.example update

- Out of scope:
  - ingest-only and worker-only builders (left as placeholders for future)
  - Separate binaries or entrypoints
  - Changes to existing tests (all existing tests pass)

## Testing
- [x] Unit tests pass: `go test ./...`
- [x] Build succeeds: `go build ./...`
- [x] Manual verification: tested SERVICE_ROLE=all and SERVICE_ROLE=http-only

### Test details
Commands run:
```bash
go build ./...
go test ./...
SERVICE_ROLE=http-only go run ./cmd/app  # verified HTTP-only starts without MQTT/Redis
```

Results:
- All packages compile without errors
- All existing tests pass (no test failures)
- http-only mode starts HTTP server successfully, skips MQTT/Redis/Worker initialization

## Reviewer focus
- **Critical files:**
  - `internal/platform/runtime/wiring.go` — role dispatch logic, buildHTTPOnly() correctness
  - `cmd/app/main.go` — conditional startup, nil checks, shutdown ordering
  - `internal/platform/config/config.go` — SERVICE_ROLE parsing and validation

- **Risk areas:**
  - Ensure nil pings in http-only mode don't cause panics (handler already handles nil)
  - Verify shutdown order: MQTT/Redis only added when non-nil
  - Check that TelemetryService is always constructed (required by handler)

## Notes
- No database migrations required
- Backward compatible: default role is `all` (existing behavior)
- Future work: implement ingest-only and worker-only builders
- Configuration change: new `SERVICE_ROLE` env var (optional, defaults to "all")
