package config

import (
	"errors"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// LoadFile reads YAML configuration into the config struct.
func LoadFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, cfg)
}

// Config holds all configuration for the telemetry service.
type Config struct {
	Service  ServiceConfig  `yaml:"service"`
	MQTT     MQTTConfig     `yaml:"mqtt"`
	Redis    RedisConfig    `yaml:"redis"`
	Postgres PostgresConfig `yaml:"postgres"`
	Profiles ProfilesConfig `yaml:"profiles"`
	Worker   WorkerConfig   `yaml:"worker"`
	HTTP     HTTPConfig     `yaml:"http"`
}

// ServiceConfig holds general service settings.
type ServiceConfig struct {
	Name       string `yaml:"name"`
	InstanceID string `yaml:"instance_id"`
	LogLevel   string `yaml:"log_level"`
}

// MQTTConfig holds MQTT connection settings.
type MQTTConfig struct {
	Broker        string        `yaml:"broker"`
	ClientID      string        `yaml:"client_id"`
	Topic         string        `yaml:"topic"`
	QOS           int           `yaml:"qos"`
	CleanSession  bool          `yaml:"clean_session"`
	Username      string        `yaml:"username"`
	Password      string        `yaml:"-"` // resolved from env
	SessionExpiry time.Duration `yaml:"session_expiry"`
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Addr             string        `yaml:"addr"`
	Password         string        `yaml:"-"` // resolved from env
	DB               int           `yaml:"db"`
	Stream           string        `yaml:"stream"`
	DeadletterStream string        `yaml:"deadletter_stream"`
	Group            string        `yaml:"group"`
	Consumer         string        `yaml:"consumer"`
	ReadCount        int           `yaml:"read_count"`
	BlockTime        time.Duration `yaml:"block_time"`
	MinIdleTime      time.Duration `yaml:"min_idle_time"`
}

// PostgresConfig holds PostgreSQL connection settings.
type PostgresConfig struct {
	DSN           string        `yaml:"-"` // resolved from env
	MaxWriteConns int           `yaml:"max_write_conns"`
	MaxReadConns  int           `yaml:"max_read_conns"`
	BatchSize     int           `yaml:"batch_size"`
	BatchTimeout  time.Duration `yaml:"batch_timeout"`
}

// ProfilesConfig holds profile registry settings.
type ProfilesConfig struct {
	Path string `yaml:"path"`
}

// WorkerConfig holds worker behavior settings.
type WorkerConfig struct {
	MaxRetries          int           `yaml:"max_retries"`
	RetryBackoffInitial time.Duration `yaml:"retry_backoff_initial"`
	RetryBackoffMax     time.Duration `yaml:"retry_backoff_max"`
}

// HTTPConfig holds HTTP server settings.
type HTTPConfig struct {
	ListenAddr string `yaml:"listen_addr"`
}

// Load reads configuration from environment and YAML file.
func Load(path string) (*Config, error) {
	cfg := Default()

	// Load YAML file if provided
	if path != "" {
		if err := LoadFile(path, cfg); err != nil {
			return nil, err
		}
	}

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
	// Load MQTT password from env
	if pwd := os.Getenv("MQTT_PASSWORD"); pwd != "" {
		cfg.MQTT.Password = pwd
	}
	// Load Redis password from env
	if pwd := os.Getenv("REDIS_PASSWORD"); pwd != "" {
		cfg.Redis.Password = pwd
	}
	// Load PostgreSQL DSN from env
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		cfg.Postgres.DSN = dsn
	}
	// Override with explicit env vars if set
	if v := os.Getenv("MQTT_BROKER"); v != "" {
		cfg.MQTT.Broker = v
	}
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		cfg.Redis.Addr = v
	}
	if v := os.Getenv("HTTP_LISTEN_ADDR"); v != "" {
		cfg.HTTP.ListenAddr = v
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.Service.LogLevel = v
	}
	if v := os.Getenv("INSTANCE_ID"); v != "" {
		cfg.Service.InstanceID = v
	}
	// Parse numeric overrides
	if v := os.Getenv("WORKER_MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Worker.MaxRetries = n
		}
	}
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Postgres.BatchSize = n
		}
	}
	return nil
}
