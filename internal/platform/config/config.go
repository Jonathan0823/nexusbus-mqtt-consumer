package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the telemetry service.
type Config struct {
	Service  ServiceConfig
	MQTT     MQTTConfig
	Redis    RedisConfig
	Postgres PostgresConfig
	Profiles ProfilesConfig
	Worker   WorkerConfig
	HTTP     HTTPConfig
	Ingest   IngestConfig
}

// ServiceRole defines the deployment mode.
type ServiceRole string

// Service role constants.
const (
	RoleAll       ServiceRole = "all"
	RoleHTTPOnly  ServiceRole = "http-only"
	RoleIngestOnly ServiceRole = "ingest-only"
	RoleWorkerOnly ServiceRole = "worker-only"
)

// ServiceConfig holds general service settings.
type ServiceConfig struct {
	Name       string
	InstanceID string
	LogLevel   string
	Role      ServiceRole
}

// setRole parses SERVICE_ROLE env var with fallback to RoleAll.
func setRole() ServiceRole {
	role := os.Getenv("SERVICE_ROLE")
	if role == "" {
		return RoleAll
	}
	switch ServiceRole(role) {
	case RoleAll, RoleHTTPOnly, RoleIngestOnly, RoleWorkerOnly:
		return ServiceRole(role)
	default:
		return RoleAll
	}
}

// MQTTConfig holds MQTT connection settings.
type MQTTConfig struct {
	Broker       string
	ClientID     string
	Topic        string
	QOS          int
	CleanSession bool
	Username     string
	Password     string // resolved from env
	// Reserved for future MQTT v5 session control; documented but not yet wired.
	SessionExpiry time.Duration
	Timeout       time.Duration
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Addr             string
	Password         string // resolved from env
	DB               int
	Stream           string
	DeadletterStream string
	Group            string
	Consumer         string
	// Reserved for future tuning of Redis reads; documented but not yet wired.
	ReadCount   int
	BlockTime   time.Duration
	MinIdleTime time.Duration
}

// PostgresConfig holds PostgreSQL connection settings.
type PostgresConfig struct {
	DSN           string // resolved from env
	MaxWriteConns int
	// Reserved for future read/write pool separation; documented but not yet wired.
	MaxReadConns int
	BatchSize    int
	BatchTimeout time.Duration
}

// ProfilesConfig holds profile registry settings.
type ProfilesConfig struct {
	Path string
}

// WorkerConfig holds worker behavior settings.
type WorkerConfig struct {
	MaxRetries int
	// Reserved for future retry backoff scheduling; documented but not yet wired.
	RetryBackoffInitial time.Duration
	RetryBackoffMax     time.Duration
}

// HTTPConfig holds HTTP server settings.
type HTTPConfig struct {
	ListenAddr string
}

// IngestConfig holds ingest behavior settings.
type IngestConfig struct {
	Mode          string        // "normal" or "coalesce"
	FlushInterval time.Duration // flush interval for coalesce mode
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	godotenv.Load()
	cfg := Default()

	// Override with environment variables
	if err := applyEnvOverrides(cfg); err != nil {
		return nil, err
	}

	// SERVICE_ROLE must be loaded after env overrides
	cfg.Service.Role = setRole()

	// Validate
	if err := Validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Default returns a config with default values.
func Default() *Config {
	return &Config{
		Service: ServiceConfig{
			Name:       "telemetry-ingestion-service",
			InstanceID: "default",
			LogLevel:   "info",
			Role:       RoleAll,
		},
		MQTT: MQTTConfig{
			Broker:        "tcp://localhost:1883",
			ClientID:      "telemetry-ingestion-service",
			Topic:         "telemetry/raw/#",
			QOS:           1,
			CleanSession:  false,
			SessionExpiry: 7 * 24 * time.Hour, // 7 days
			Timeout:       30 * time.Second,
		},
		Redis: RedisConfig{
			Addr:             "localhost:6379",
			DB:               0,
			Stream:           "telemetry_stream",
			DeadletterStream: "telemetry_deadletter_stream",
			Group:            "telemetry_workers",
			ReadCount:        500,
			BlockTime:        time.Second,
			MinIdleTime:      60 * time.Second,
		},
		Postgres: PostgresConfig{
			MaxWriteConns: 10,
			MaxReadConns:  5,
			BatchSize:     500,
			BatchTimeout:  time.Second,
		},
		Profiles: ProfilesConfig{
			Path: "./profiles.yaml",
		},
		Worker: WorkerConfig{
			MaxRetries:          5,
			RetryBackoffInitial: time.Second,
			RetryBackoffMax:     60 * time.Second,
		},
		HTTP: HTTPConfig{
			ListenAddr: ":8080",
		},
		Ingest: IngestConfig{
			Mode:          "normal",
			FlushInterval: 30 * time.Second,
		},
	}
}

// Validate checks required fields.
func Validate(cfg *Config) error {
	if cfg.MQTT.Broker == "" {
		return errors.New("mqtt.broker is required")
	}
	if cfg.Redis.Addr == "" {
		return errors.New("redis.addr is required")
	}
	if cfg.Postgres.DSN == "" {
		return errors.New("postgres DSN is required")
	}
	if cfg.Ingest.Mode != "normal" && cfg.Ingest.Mode != "coalesce" {
		return errors.New("ingest.mode must be 'normal' or 'coalesce'")
	}
	if cfg.Ingest.FlushInterval <= 0 {
		return errors.New("ingest.flush_interval must be positive")
	}
	return nil
}

func setIntEnv(key, raw string, dst *int) error {
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	*dst = n
	return nil
}

func setDurationEnv(key, raw string, dst *time.Duration) error {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	*dst = d
	return nil
}

func setBoolEnv(key, raw string, dst *bool) error {
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	*dst = b
	return nil
}

// applyEnvOverrides applies environment variable overrides.
func applyEnvOverrides(cfg *Config) error {
	if v := os.Getenv("SERVICE_NAME"); v != "" {
		cfg.Service.Name = v
	}
	if v := os.Getenv("INSTANCE_ID"); v != "" {
		cfg.Service.InstanceID = v
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.Service.LogLevel = v
	}
	if v := os.Getenv("HTTP_LISTEN_ADDR"); v != "" {
		cfg.HTTP.ListenAddr = v
	}

	if v := os.Getenv("MQTT_URL"); v != "" {
		cfg.MQTT.Broker = v
	} else if v := os.Getenv("MQTT_BROKER"); v != "" {
		cfg.MQTT.Broker = v
	}
	if v := os.Getenv("MQTT_CLIENT_ID"); v != "" {
		cfg.MQTT.ClientID = v
	}
	if v := os.Getenv("MQTT_TOPIC"); v != "" {
		cfg.MQTT.Topic = v
	}
	if v := os.Getenv("MQTT_QOS"); v != "" {
		if err := setIntEnv("MQTT_QOS", v, &cfg.MQTT.QOS); err != nil {
			return err
		}
	}
	if v := os.Getenv("MQTT_CLEAN_SESSION"); v != "" {
		if err := setBoolEnv("MQTT_CLEAN_SESSION", v, &cfg.MQTT.CleanSession); err != nil {
			return err
		}
	}
	if v := os.Getenv("MQTT_USERNAME"); v != "" {
		cfg.MQTT.Username = v
	}
	if v := os.Getenv("MQTT_PASSWORD"); v != "" {
		cfg.MQTT.Password = v
	}
	if v := os.Getenv("MQTT_SESSION_EXPIRY"); v != "" {
		if err := setDurationEnv("MQTT_SESSION_EXPIRY", v, &cfg.MQTT.SessionExpiry); err != nil {
			return err
		}
	}
	if v := os.Getenv("MQTT_TIMEOUT"); v != "" {
		if err := setDurationEnv("MQTT_TIMEOUT", v, &cfg.MQTT.Timeout); err != nil {
			return err
		}
	}

	if v := os.Getenv("REDIS_URL"); v != "" {
		cfg.Redis.Addr = v
	} else if v := os.Getenv("REDIS_ADDR"); v != "" {
		cfg.Redis.Addr = v
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		cfg.Redis.Password = v
	}
	if v := os.Getenv("REDIS_DB"); v != "" {
		if err := setIntEnv("REDIS_DB", v, &cfg.Redis.DB); err != nil {
			return err
		}
	}
	if v := os.Getenv("REDIS_STREAM"); v != "" {
		cfg.Redis.Stream = v
	}
	if v := os.Getenv("REDIS_DEADLETTER_STREAM"); v != "" {
		cfg.Redis.DeadletterStream = v
	}
	if v := os.Getenv("REDIS_GROUP"); v != "" {
		cfg.Redis.Group = v
	}
	if v := os.Getenv("REDIS_CONSUMER"); v != "" {
		cfg.Redis.Consumer = v
	}
	if v := os.Getenv("REDIS_READ_COUNT"); v != "" {
		if err := setIntEnv("REDIS_READ_COUNT", v, &cfg.Redis.ReadCount); err != nil {
			return err
		}
	}
	if v := os.Getenv("REDIS_BLOCK_TIME"); v != "" {
		if err := setDurationEnv("REDIS_BLOCK_TIME", v, &cfg.Redis.BlockTime); err != nil {
			return err
		}
	}
	if v := os.Getenv("REDIS_MIN_IDLE_TIME"); v != "" {
		if err := setDurationEnv("REDIS_MIN_IDLE_TIME", v, &cfg.Redis.MinIdleTime); err != nil {
			return err
		}
	}

	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		cfg.Postgres.DSN = v
	}
	if v := os.Getenv("POSTGRES_MAX_WRITE_CONNS"); v != "" {
		if err := setIntEnv("POSTGRES_MAX_WRITE_CONNS", v, &cfg.Postgres.MaxWriteConns); err != nil {
			return err
		}
	}
	if v := os.Getenv("POSTGRES_MAX_READ_CONNS"); v != "" {
		if err := setIntEnv("POSTGRES_MAX_READ_CONNS", v, &cfg.Postgres.MaxReadConns); err != nil {
			return err
		}
	}
	if v := os.Getenv("POSTGRES_BATCH_SIZE"); v != "" {
		if err := setIntEnv("POSTGRES_BATCH_SIZE", v, &cfg.Postgres.BatchSize); err != nil {
			return err
		}
	}
	if v := os.Getenv("POSTGRES_BATCH_TIMEOUT"); v != "" {
		if err := setDurationEnv("POSTGRES_BATCH_TIMEOUT", v, &cfg.Postgres.BatchTimeout); err != nil {
			return err
		}
	}

	if v := os.Getenv("WORKER_MAX_RETRIES"); v != "" {
		if err := setIntEnv("WORKER_MAX_RETRIES", v, &cfg.Worker.MaxRetries); err != nil {
			return err
		}
	}
	if v := os.Getenv("WORKER_RETRY_BACKOFF_INITIAL"); v != "" {
		if err := setDurationEnv("WORKER_RETRY_BACKOFF_INITIAL", v, &cfg.Worker.RetryBackoffInitial); err != nil {
			return err
		}
	}
	if v := os.Getenv("WORKER_RETRY_BACKOFF_MAX"); v != "" {
		if err := setDurationEnv("WORKER_RETRY_BACKOFF_MAX", v, &cfg.Worker.RetryBackoffMax); err != nil {
			return err
		}
	}

	if v := os.Getenv("PROFILES_PATH"); v != "" {
		cfg.Profiles.Path = v
	}
	if v := os.Getenv("INGEST_MODE"); v != "" {
		cfg.Ingest.Mode = v
	}
	if v := os.Getenv("INGEST_FLUSH_INTERVAL"); v != "" {
		if err := setDurationEnv("INGEST_FLUSH_INTERVAL", v, &cfg.Ingest.FlushInterval); err != nil {
			return err
		}
	}
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if err := setIntEnv("BATCH_SIZE", v, &cfg.Postgres.BatchSize); err != nil {
			return err
		}
	}
	return nil
}
