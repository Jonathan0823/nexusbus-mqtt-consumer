package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	httphandler "modbus-mqtt-consumer/internal/adapters/http"
	"modbus-mqtt-consumer/internal/adapters/mqtt"
	"modbus-mqtt-consumer/internal/adapters/postgres"
	"modbus-mqtt-consumer/internal/adapters/redis"
	"modbus-mqtt-consumer/internal/adapters/yamlprofile"
	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/service"
	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/metrics"
	"modbus-mqtt-consumer/internal/platform/shutdown"
)

func main() {
	// Load configuration (YAML + env overrides + validation)
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config load error: %v\n", err)
		os.Exit(1)
	}

	// Logger
	logger := logging.New(cfg.Service.LogLevel)
	logger.Info("starting telemetry service", "config", configPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Initialize Shutdown Manager ---
	shutdownMgr := shutdown.NewManager(logger, 30*time.Second)

	// --- Metrics ---
	recorder := metrics.NewRecorder()

	// --- Redis Stream Buffer ---
	redisBuffer, err := redis.NewStreamBuffer(redis.Config{
		Addr:             cfg.Redis.Addr,
		Password:         cfg.Redis.Password,
		DB:               cfg.Redis.DB,
		Stream:           cfg.Redis.Stream,
		DeadletterStream: cfg.Redis.DeadletterStream,
		Group:            cfg.Redis.Group,
		Consumer:         cfg.Redis.Consumer,
	}, logger)
	if err != nil {
		logger.Error("redis connection failed", "error", err)
		os.Exit(1)
	}

	// --- PostgreSQL Repository ---
	postgresRepo, err := postgres.NewRepository(ctx, postgres.Config{
		DSN:           cfg.Postgres.DSN,
		MaxWriteConns: cfg.Postgres.MaxWriteConns,
		MaxReadConns:  cfg.Postgres.MaxReadConns,
	}, logger)
	if err != nil {
		logger.Error("postgres connection failed", "error", err)
		os.Exit(1)
	}
	defer postgresRepo.Close()

	// Ensure schema
	if err := postgresRepo.EnsureSchema(ctx); err != nil {
		logger.Error("postgres schema creation failed", "error", err)
		os.Exit(1)
	}

	// --- Profile Registry ---
	profileRegistry, err := yamlprofile.NewRegistry(cfg.Profiles.Path, logger)
	if err != nil {
		logger.Error("profile registry load failed", "error", err)
		os.Exit(1)
	}

	// --- Ingest Service ---
	ingestSvc := service.NewIngestService(redisBuffer, logger)

	// --- MQTT Subscriber ---
	mqttSub := mqtt.NewSubscriber(mqtt.MQTTConfig{
		Broker:       cfg.MQTT.Broker,
		ClientID:     cfg.MQTT.ClientID,
		Topic:        cfg.MQTT.Topic,
		QOS:          byte(cfg.MQTT.QOS),
		CleanSession: cfg.MQTT.CleanSession,
		Username:     cfg.MQTT.Username,
		Password:     cfg.MQTT.Password,
		Timeout:      30 * time.Second,
	}, logger)

	// Wire up MQTT message handler
	if err := mqttSub.Subscribe(ctx, func(payload domain.RawTelemetryPayload) error {
		recorder.IncMQTTReceived()
		return ingestSvc.Handle(ctx, payload)
	}); err != nil {
		logger.Error("mqtt subscribe failed", "error", err)
		os.Exit(1)
	}

	// --- Worker Service ---
	workerSvc := service.NewWorkerService(
		redisBuffer,
		postgresRepo,
		profileRegistry,
		recorder,
		logger,
		cfg.Worker.MaxRetries,
	)

	// --- HTTP Handlers ---
	httpHandler := httphandler.NewHandler(
		logger,
		recorder,
		func() error { return redisBuffer.Ping(ctx) },
		func() error { return postgresRepo.Ping(ctx) },
		true,
	)

	// Start HTTP server
	httpServer := &http.Server{
		Addr:    cfg.HTTP.ListenAddr,
		Handler: httpHandler,
	}

	go func() {
		logger.Info("http server starting", "addr", cfg.HTTP.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", "error", err)
		}
	}()

	// --- Add shutdown components ---
	shutdownMgr.Add(&serviceComponent{name: "mqtt", closer: mqttSub})
	shutdownMgr.Add(&serviceComponent{name: "redis", closer: redisBuffer})
	shutdownMgr.Add(&serviceComponent{name: "http", closer: &httpServerCloser{server: httpServer}})

	// --- Start Worker Loop ---
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := workerSvc.ProcessBatch(ctx, cfg.Postgres.BatchSize); err != nil {
					logger.Error("worker batch failed", "error", err)
				}

				// Also recover stale messages periodically
				if err := workerSvc.RecoverStale(ctx, cfg.Redis.MinIdleTime, 100); err != nil {
					logger.Error("recover stale failed", "error", err)
				}
			}
		}
	}()

	// --- Run Shutdown Manager ---
	shutdownMgr.Run(ctx)
	<-shutdownMgr.Done()

	logger.Info("service stopped")
}

// serviceComponent wraps a closer for shutdown.Manager.
type serviceComponent struct {
	name   string
	closer interface {
		Close() error
	}
}

func (c *serviceComponent) Close() error {
	return c.closer.Close()
}

func (c *serviceComponent) Name() string {
	return c.name
}

// httpServerCloser wraps http.Server for shutdown.
type httpServerCloser struct {
	server *http.Server
}

func (c *httpServerCloser) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.server.Shutdown(ctx)
}

func (c *httpServerCloser) Name() string {
	return "http-server"
}
