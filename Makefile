.PHONY: migrate-up migrate-down migrate-create migrate-status

# Database URL - set via environment or .env
POSTGRES_DSN ?= $(shell grep -v '^#' .env 2>/dev/null | grep '^POSTGRES_DSN=' | cut -d'=' -f2-)

# Migration binary path (override if needed)
MIGRATE_BIN ?= ~/go/bin/migrate

# Install migrate CLI locally
migrate-install:
	go install github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# Run all pending up migrations
migrate-up:
	@if [ -z "$(POSTGRES_DSN)" ]; then \
		echo "POSTGRES_DSN not set. Set it via export POSTGRES_DSN=... or in .env"; \
		exit 1; \
	fi
	$(MIGRATE_BIN) -path=migrations -database "$(POSTGRES_DSN)" up

# Run all down migrations (rollback)
migrate-down:
	@if [ -z "$(POSTGRES_DSN)" ]; then \
		echo "POSTGRES_DSN not set. Set it via export POSTGRES_DSN=... or in .env"; \
		exit 1; \
	fi
	$(MIGRATE_BIN) -path=migrations -database "$(POSTGRES_DSN)" down

# Force migrate to a specific version (use with caution)
migrate-force:
	@if [ -z "$(POSTGRES_DSN)" ]; then \
		echo "POSTGRES_DSN not set."; \
		exit 1; \
	fi
	@if [ -z "$(VERSION)" ]; then \
		echo "VERSION required. Usage: make migrate-force VERSION=1"; \
		exit 1; \
	fi
	$(MIGRATE_BIN) -path=migrations -database "$(POSTGRES_DSN)" force $(VERSION)

# Show current migration status
migrate-status:
	@if [ -z "$(POSTGRES_DSN)" ]; then \
		echo "POSTGRES_DSN not set."; \
		exit 1; \
	fi
	$(MIGRATE_BIN) -path=migrations -database "$(POSTGRES_DSN)" version

# Create a new migration file (TEMPLATE: name=create_users)
migrate-create:
	@if [ -z "$(name)" ]; then \
		echo "name required. Usage: make migrate-create name=create_users"; \
		exit 1; \
	fi
	@timestamp=$$(date +%Y%m%d%H%M%S); \
	upfile="migrations/$${timestamp}_$(name).up.sql"; \
	downfile="migrations/$${timestamp}_$(name).down.sql"; \
	echo "-- migrate:up" > "$$upfile"; \
	echo "" >> "$$upfile"; \
	echo "-- migrate:down" > "$$downfile"; \
	echo "" >> "$$downfile"; \
	echo "Created $$upfile and $$downfile"; \
	ls -la "$$upfile" "$$downfile"
