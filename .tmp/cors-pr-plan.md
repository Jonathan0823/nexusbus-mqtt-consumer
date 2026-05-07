## Related issue
- No issue linked yet

## Summary
- Added Gin CORS middleware with strict allowlist (no wildcards, no credentials)
- Env-driven config via `CORS_ALLOWED_ORIGINS` (comma-separated list)
- Preflight (`OPTIONS`) handling with proper headers
- Applied CORS to all routes: `/healthz`, `/readyz`, `/metrics`, `/api/v1/devices/:device_id/telemetry`

## Scope
- In scope:
  - `internal/adapters/http/middleware.go` — CORS middleware (extracted to separate file)
  - `internal/adapters/http/routes.go` — CORS middleware usage in engine setup
  - `internal/adapters/http/cors_test.go` — CORS test suite
  - `internal/platform/config/config.go` — CORS config and env parsing
  - `internal/platform/runtime/wiring.go` — CORS config injection
- Out of scope:
  - Logging/request ID middleware (in future PR)
  - Credentials support (explicitly disabled per requirements)

## Testing
- [x] Unit tests pass for HTTP adapter package (11 tests)
- [x] Full build succeeds
- [x] All packages pass

### Test details
- Commands run:
  - `go test ./internal/adapters/http/... -v`
  - `go test ./...`
- Results:
  - All 11 HTTP tests pass
  - All packages pass

## Reviewer focus
- Please review:
  - `internal/adapters/http/routes.go` — CORS middleware logic and security model
  - `internal/platform/config/config.go` — Config design and env parsing
- Risk areas:
  - Disallowed origin handling (should not expose data, returns 403 for preflight)
  - No credentials means `Access-Control-Allow-Credentials` is never set

## Notes
- Migrations/config changes:
  - New env var: `CORS_ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000`
- Follow-ups: 
  - Add logging/request ID middleware in separate PR
  - Consider metrics middleware later

**Files:**
- `internal/adapters/http/middleware.go` — CORS middleware (separate file)
- `internal/adapters/http/routes.go` — uses corsMiddleware in engine
- etc.