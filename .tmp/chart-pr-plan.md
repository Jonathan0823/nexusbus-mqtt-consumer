## Related issue
- No issue linked yet

## Summary
- New `GET /api/v1/devices/:device_id/chart` endpoint for frontend charting
- Returns `{metric, points: [{x, y}]}` format optimized for Recharts
- One series per metric, suitable for 1 chart per metric requirement
- Keeps existing `/telemetry` endpoint unchanged

## Scope
- In scope:
  - `internal/adapters/http/handler.go` — chart types and GetChart handler
  - `internal/adapters/http/routes.go` — chart route registration
  - `internal/adapters/http/handler_test.go` — 3 new tests
- Out of scope:
  - `step` / bucket aggregation (raw points only for now)
  - `timezone` param (not needed yet)
  - `agg` (frontend can calculate manually)

## Testing
- [x] Unit tests pass for HTTP adapter package
- [x] Full build succeeds
- [x] All packages pass

### Test details
- Commands run:
  - `go test ./internal/adapters/http/... -v`
  - `go build ./...`
  - `go test ./...`
- Results:
  - All 9 HTTP tests pass (6 original + 3 new chart tests)
  - Build succeeds with no errors
  - All packages pass

## Reviewer focus
- Please review:
  - Response shape: `{data: [{metric, points: [{x, y}]}]}` — works for Recharts
  - Metric grouping — ensure it matches expected 1-series-per-metric behavior
  - Metrics filter — validates correctly when `metrics` param is specified
- Risk areas:
  - Map iteration order — controlled by `metricKeys` when filtering

## Notes
- Migrations/config changes: None
- Follow-ups: Add `step` for downsampling, `timezone` for bucketing, `agg` for backend aggregation in separate PRs
- Existing `/api/v1/devices/:device_id/telemetry` endpoint remains unchanged