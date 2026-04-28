package config

import (
	"errors"
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
}

// ServiceConfig holds general service settings.
type ServiceConfig struct {
	Name       string
	InstanceID string
	LogLevel   string
}

// MQTTConfig holds MQTT connection settings.
type MQTTConfig struct {
	Broker        string
	ClientID      string
	Topic         string
	QOS           int
	CleanSession  bool
	Username      string
	Password      string // resolved from env
	SessionExpiry time.Duration
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
	ReadCount        int
	BlockTime        time.Duration
	MinIdleTime      time.Duration
}

// PostgresConfig holds PostgreSQL connection settings.
type PostgresConfig struct {
	DSN           string // resolved from env
	MaxWriteConns int
	MaxReadConns  int
	BatchSize     int
	BatchTimeout  time.Duration
}

// ProfilesConfig holds profile registry settings.
type ProfilesConfig struct {
	Path string
}

// WorkerConfig holds worker behavior settings.
type WorkerConfig struct {
	MaxRetries          int
	RetryBackoffInitial time.Duration
	RetryBackoffMax     time.Duration
}

// HTTPConfig holds HTTP server settings.
type HTTPConfig struct {
	ListenAddr string
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	godotenv.Load()
	cfg := Default()

	// Override with environment variables
	if err := applyEnvOverrides(cfg); err != nil {
		return nil, err
	}

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
		},
		MQTT: MQTTConfig{
			Broker:        "tcp://localhost:1883",
			ClientID:      "telemetry-ingestion-service",
			Topic:         "telemetry/raw/#",
			QOS:           1,
			CleanSession:  false,
			SessionExpiry: 7 * 24 * time.Hour, // 7 days
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
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MQTT.QOS = n
		}
	}
	if v := os.Getenv("MQTT_CLEAN_SESSION"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.MQTT.CleanSession = b
		}
	}
	if v := os.Getenv("MQTT_USERNAME"); v != "" {
		cfg.MQTT.Username = v
	}
	if v := os.Getenv("MQTT_PASSWORD"); v != "" {
		cfg.MQTT.Password = v
	}
	if v := os.Getenv("MQTT_SESSION_EXPIRY"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.MQTT.SessionExpiry = d
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
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Redis.DB = n
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
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Redis.ReadCount = n
		}
	}
	if v := os.Getenv("REDIS_BLOCK_TIME"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Redis.BlockTime = d
		}
	}
	if v := os.Getenv("REDIS_MIN_IDLE_TIME"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Redis.MinIdleTime = d
		}
	}

	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		cfg.Postgres.DSN = v
	}
	if v := os.Getenv("POSTGRES_MAX_WRITE_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Postgres.MaxWriteConns = n
		}
	}
	if v := os.Getenv("POSTGRES_MAX_READ_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Postgres.MaxReadConns = n
		}
	}
	if v := os.Getenv("POSTGRES_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Postgres.BatchSize = n
		}
	}
	if v := os.Getenv("POSTGRES_BATCH_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Postgres.BatchTimeout = d
		}
	}

	if v := os.Getenv("WORKER_MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Worker.MaxRetries = n
		}
	}
	if v := os.Getenv("WORKER_RETRY_BACKOFF_INITIAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Worker.RetryBackoffInitial = d
		}
	}
	if v := os.Getenv("WORKER_RETRY_BACKOFF_MAX"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Worker.RetryBackoffMax = d
		}
	}

	if v := os.Getenv("PROFILES_PATH"); v != "" {
		cfg.Profiles.Path = v
	}
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Postgres.BatchSize = n
		}
	}
	return nil
}
