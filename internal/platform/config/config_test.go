package config

import (
	"strings"
	"testing"
)

func TestNormalizeBasePath(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"/", ""},
		{"/telemetry", "/telemetry"},
		{"/telemetry/", "/telemetry"},
		{"telemetry", "/telemetry"},
		{"  /telemetry  ", "/telemetry"},
		{"//telemetry", "/telemetry"},
		{"/telemetry/api", "/telemetry/api"},
		{"///telemetry", "/telemetry"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got := normalizeBasePath(tc.input)
			if got != tc.expected {
				t.Errorf("normalizeBasePath(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestLoad_HTTPBasePathEnv(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost:5432/test?sslmode=disable")
	t.Setenv("HTTP_BASE_PATH", "/telemetry")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.HTTP.BasePath != "/telemetry" {
		t.Errorf("got BasePath %q, want /telemetry", cfg.HTTP.BasePath)
	}
}

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
