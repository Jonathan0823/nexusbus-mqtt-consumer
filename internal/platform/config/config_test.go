package config

import (
	"strings"
	"testing"
)

func TestLoad_InvalidTypedEnvOverrides(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
	}{
		{name: "mqtt qos", key: "MQTT_QOS", value: "not-an-int"},
		{name: "mqtt clean session", key: "MQTT_CLEAN_SESSION", value: "not-a-bool"},
		{name: "redis block time", key: "REDIS_BLOCK_TIME", value: "not-a-duration"},
		{name: "ingest flush interval", key: "INGEST_FLUSH_INTERVAL", value: "not-a-duration"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost:5432/test?sslmode=disable")
			t.Setenv(tc.key, tc.value)

			_, err := Load()
			if err == nil {
				t.Fatalf("expected Load to fail for %s", tc.key)
			}

			if !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("expected error to mention %s, got %v", tc.key, err)
			}
		})
	}
}
