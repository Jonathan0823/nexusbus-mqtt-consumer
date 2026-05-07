# Task Context: HTTP Gin Migration

Session ID: 2026-05-07-http-gin-migration
Created: 2026-05-07T00:00:00Z
Status: in_progress

## Current Request
Replace native net/http HTTP layer with Gin while maintaining semantic behavior.

## Context Files (Standards to Follow)
- `/home/ejo4358/.config/opencode/context/core/standards/code-quality.md` — Small functions, explicit dependencies
- `/home/ejo4358/.config/opencode/context/development/principles/clean-code.md` — Go clean code for handlers
- `/home/ejo4358/.config/opencode/context/development/principles/api-design.md` — HTTP semantics, status codes, route design

## Reference Files (Source Material)
- `internal/adapters/http/handler.go` — Current handler implementation
- `internal/adapters/http/routes.go` — Current route setup
- `internal/adapters/http/handler_test.go` — Current tests
- `internal/platform/runtime/wiring.go` — Server bootstrap

## Components
1. Rewrite handler.go with `*gin.Context`
2. Rewrite routes.go with Gin engine
3. Update wiring.go for Gin server
4. Update tests for Gin
5. Add Gin dependency

## Constraints
- Semantic compatibility for status codes and response fields
- Exact same routes: `/healthz`, `/readyz`, `/metrics`, `/api/v1/devices/:device_id/telemetry`
- Don't break existing tests

## Exit Criteria
- [ ] Tests pass with Gin-based requests
- [ ] All HTTP endpoints respond correctly
- [ ] No net/http imports in handler or routes