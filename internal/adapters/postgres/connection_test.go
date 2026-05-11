package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"modbus-mqtt-consumer/internal/platform/config"
)

func TestNewPool_LogsDatabaseNameFromURIDSN(t *testing.T) {
	t.Parallel()

	cfg := config.PostgresConfig{
		DSN:           "postgres://postgres:secret@10.19.16.31:5432/iot_db?sslmode=disable",
		MaxWriteConns: 10,
		MaxReadConns:  5,
		BatchSize:     500,
		BatchTimeout:  0,
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}

	if got := poolConfig.ConnConfig.Database; got != "iot_db" {
		t.Fatalf("ConnConfig.Database = %q, want iot_db", got)
	}
}

func TestKeywordDSNDatabaseNameIsParsedByPgxpool(t *testing.T) {
	t.Parallel()

	cfg, err := pgxpool.ParseConfig("host=localhost port=5432 dbname=iot_db user=postgres password=secret")
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}

	if got := cfg.ConnConfig.Database; got != "iot_db" {
		t.Fatalf("ConnConfig.Database = %q, want iot_db", got)
	}
}

func TestNewPool_LogsDatabaseNameFromKeywordDSN(t *testing.T) {
	t.Parallel()

	cfg := config.PostgresConfig{
		DSN:           "host=localhost port=5432 dbname=test_db user=postgres password=secret",
		MaxWriteConns: 10,
		MaxReadConns:  5,
		BatchSize:     500,
		BatchTimeout:  0,
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}

	if got := poolConfig.ConnConfig.Database; got != "test_db" {
		t.Fatalf("ConnConfig.Database = %q, want test_db", got)
	}
}
