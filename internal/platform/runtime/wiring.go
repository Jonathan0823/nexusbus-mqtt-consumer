package runtime

import (
	"context"
	"fmt"
	"net/http"
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
)

// Wiring holds all component dependencies.
type Wiring struct {
	// Config and logger
	Cfg    *config.Config
	Logger *logging.Logger

	// Core services
	IngestService *service.IngestService
	WorkerService *service.WorkerService

	// Adapters
	MQTTSubscriber *mqtt.Subscriber
	RedisBuffer    *redis.StreamBuffer
	PostgresRepo   *postgres.Repository
	ProfileReg     *yamlprofile.Registry
	HTTPHandler    *httphandler.Handler

	// Metrics
	Metrics *metrics.Recorder

	// HTTP server reference for graceful shutdown
	HTTPServer *http.Server
}

// NewWiring creates and wires all dependencies.
func NewWiring(ctx context.Context, cfg *config.Config, logger *logging.Logger) (*Wiring, error) {
	w := &Wiring{
		Cfg:    cfg,
		Logger: logger,
	}

	// Metrics recorder
	w.Metrics = metrics.NewRecorder()

	// Redis Stream Buffer
	redisBuf, err := redis.NewStreamBuffer(redis.Config{
		Addr:             cfg.Redis.Addr,
		Password:         cfg.Redis.Password,
		DB:               cfg.Redis.DB,
		Stream:           cfg.Redis.Stream,
		DeadletterStream: cfg.Redis.DeadletterStream,
		Group:            cfg.Redis.Group,
		Consumer:         cfg.Redis.Consumer,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("redis connection: %w", err)
	}
	w.RedisBuffer = redisBuf

	// PostgreSQL Repository
	postgresRepo, err := postgres.NewRepository(ctx, postgres.Config{
		DSN:           cfg.Postgres.DSN,
		MaxWriteConns: cfg.Postgres.MaxWriteConns,
		MaxReadConns:  cfg.Postgres.MaxReadConns,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("postgres connection: %w", err)
	}
	w.PostgresRepo = postgresRepo

	// Ensure schema
	if err := postgresRepo.EnsureSchema(ctx); err != nil {
		return nil, fmt.Errorf("postgres schema: %w", err)
	}

	// Profile Registry
	profileReg, err := yamlprofile.NewRegistry(cfg.Profiles.Path, logger)
	if err != nil {
		return nil, fmt.Errorf("profile registry: %w", err)
	}
	w.ProfileReg = profileReg

	// Ingest Service
	w.IngestService = service.NewIngestService(w.RedisBuffer, logger)

	// MQTT Subscriber
	w.MQTTSubscriber = mqtt.NewSubscriber(mqtt.MQTTConfig{
		Broker:       cfg.MQTT.Broker,
		ClientID:     cfg.MQTT.ClientID,
		Topic:        cfg.MQTT.Topic,
		QOS:          byte(cfg.MQTT.QOS),
		CleanSession: cfg.MQTT.CleanSession,
		Username:     cfg.MQTT.Username,
		Password:     cfg.MQTT.Password,
		Timeout:      30 * time.Second,
	}, logger)

	// Worker Service
	w.WorkerService = service.NewWorkerService(
		w.RedisBuffer,
		w.PostgresRepo,
		w.ProfileReg,
		w.Metrics,
		logger,
		cfg.Worker.MaxRetries,
	)

	// HTTP Handler
	w.HTTPHandler = httphandler.NewHandler(
		logger,
		w.Metrics,
		func() error { return w.RedisBuffer.Ping(ctx) },
		func() error { return w.PostgresRepo.Ping(ctx) },
		true,
	)

	// HTTP Server
	w.HTTPServer = &http.Server{
		Addr:    cfg.HTTP.ListenAddr,
		Handler: w.HTTPHandler,
	}

	return w, nil
}

// StartMQTT starts the MQTT subscriber with message handling.
func (w *Wiring) StartMQTT(ctx context.Context) error {
	// Wire up MQTT message handler
	if err := w.MQTTSubscriber.Subscribe(ctx, func(payload domain.RawTelemetryPayload) error {
		w.Metrics.IncMQTTReceived()
		return w.IngestService.Handle(ctx, payload)
	}); err != nil {
		return fmt.Errorf("mqtt subscribe: %w", err)
	}

	w.Logger.Info("mqtt subscriber started")
	return nil
}

// StartHTTPServer starts the HTTP server in a goroutine.
func (w *Wiring) StartHTTPServer() error {
	go func() {
		w.Logger.Info("http server starting", "addr", w.HTTPServer.Addr)
		if err := w.HTTPServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			w.Logger.Error("http server failed", "error", err)
		}
	}()
	return nil
}

// StartWorkerLoop starts the background worker processing loop.
func (w *Wiring) StartWorkerLoop(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.WorkerService.ProcessBatch(ctx, w.Cfg.Postgres.BatchSize); err != nil {
					w.Logger.Error("worker batch failed", "error", err)
				}

				// Also recover stale messages periodically
				if err := w.WorkerService.RecoverStale(ctx, w.Cfg.Redis.MinIdleTime, 100); err != nil {
					w.Logger.Error("recover stale failed", "error", err)
				}
			}
		}
	}()
}

// Close closes all resources in reverse order.
func (w *Wiring) Close() error {
	w.Logger.Info("closing wiring resources")

	w.PostgresRepo.Close()
	w.RedisBuffer.Close()

	return nil
}
