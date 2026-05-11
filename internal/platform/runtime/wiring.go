package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	httphandler "modbus-mqtt-consumer/internal/adapters/http"
	mqttadapter "modbus-mqtt-consumer/internal/adapters/mqtt"
	"modbus-mqtt-consumer/internal/adapters/postgres"
	redisadapter "modbus-mqtt-consumer/internal/adapters/redis"
	"modbus-mqtt-consumer/internal/adapters/yamlprofile"
	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/ports"
	"modbus-mqtt-consumer/internal/core/service"
	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/metrics"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Wiring holds all component dependencies.
// Components are constructed conditionally based on role.
type Wiring struct {
	// Config and logger
	Cfg    *config.Config
	Logger *logging.Logger

	// Role determines which components are constructed
	Role config.ServiceRole

	// Core services (may be nil depending on role)
	IngestService    *service.IngestService
	WorkerService    *service.WorkerService
	CoalescingWorker *service.CoalescingWorker
	TelemetryService ports.TelemetryService

	// Adapters (may be nil depending on role)
	MQTTSubscriber *mqttadapter.Subscriber
	RedisBuffer    *redisadapter.StreamBuffer
	ChartCache     *redisadapter.ChartCacheAdapter
	PostgresRepo   *postgres.Repository
	ProfileReg     *yamlprofile.Registry
	HTTPHandler    *httphandler.Handler

	// Metrics
	Metrics *metrics.Recorder

	// HTTP server reference for graceful shutdown
	HTTPServer *http.Server
}

// NewWiring creates and wires all dependencies based on role.
func NewWiring(ctx context.Context, cfg *config.Config, logger *logging.Logger) (*Wiring, error) {
	switch cfg.Service.Role {
	case config.RoleHTTPOnly:
		return buildHTTPOnly(ctx, cfg, logger)
	case config.RoleAll, config.RoleIngestOnly, config.RoleWorkerOnly:
		return buildAll(ctx, cfg, logger)
	default:
		return buildAll(ctx, cfg, logger)
	}
}

// buildAll creates wiring for full stack (all components).
func buildAll(ctx context.Context, cfg *config.Config, logger *logging.Logger) (*Wiring, error) {
	w := &Wiring{
		Cfg:    cfg,
		Logger: logger,
		Role:   cfg.Service.Role,
	}

	// Metrics recorder
	w.Metrics = metrics.NewRecorder()

	// Redis client (shared for stream buffer and chart cache)
	redisClient, err := redisadapter.NewClient(cfg.Redis, logger)
	if err != nil {
		return nil, fmt.Errorf("redis client: %w", err)
	}

	// Redis Stream Buffer
	redisBuf, err := redisadapter.NewStreamBuffer(redisClient, cfg.Redis, logger)
	if err != nil {
		_ = redisClient.Close()
		return nil, fmt.Errorf("redis buffer: %w", err)
	}
	w.RedisBuffer = redisBuf

	// Redis Chart Cache (optional - don't fail if Redis is unavailable)
	chartCache := redisadapter.NewChartCache(redisClient, logger)
	w.ChartCache = chartCache
	logger.Info("chart cache initialized")

	// PostgreSQL Repository
	postgresPool, err := postgres.NewPool(ctx, cfg.Postgres, logger)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}

	postgresRepo := postgres.NewRepository(postgresPool, logger)
	w.PostgresRepo = postgresRepo

	// Profile Registry
	profileReg, err := yamlprofile.NewRegistry(cfg.Profiles.Path, logger)
	if err != nil {
		return nil, fmt.Errorf("profile registry: %w", err)
	}
	w.ProfileReg = profileReg

	// Ingest Service
	w.IngestService = service.NewIngestService(w.RedisBuffer, logger)

	// MQTT Client with auto-reconnect and per-reconnect resubscribe.
	onConnected := func(client mqtt.Client) {
		qos := byte(cfg.MQTT.QOS)
		token := client.Subscribe(cfg.MQTT.Topic, qos, func(_ mqtt.Client, msg mqtt.Message) {
			var payload domain.RawTelemetryPayload
			if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
				logger.Error("mqtt message parse error", "error", err)
				return
			}
			w.Metrics.IncMQTTReceived()
			if err := w.IngestService.Handle(ctx, payload); err != nil {
				logger.Error("mqtt message handler error", "error", err)
			}
		})
		if token.Wait() && token.Error() != nil {
			logger.Error("mqtt resubscribe failed", "error", token.Error())
			return
		}
		logger.Info("mqtt subscribed (resubscribe)", "topic", cfg.MQTT.Topic)
	}
	mqttClient, err := mqttadapter.NewClient(cfg.MQTT, logger, onConnected)
	if err != nil {
		return nil, fmt.Errorf("mqtt client: %w", err)
	}

	// MQTT Subscriber (used for connection state tracking only)
	w.MQTTSubscriber = mqttadapter.NewSubscriber(cfg.MQTT, mqttClient, logger)

	// Worker Service
	w.WorkerService = service.NewWorkerService(
		w.RedisBuffer,
		w.PostgresRepo,
		w.ProfileReg,
		w.Metrics,
		logger,
		cfg.Worker.MaxRetries,
	)

	// Coalescing Worker (for coalesce mode)
	w.CoalescingWorker = service.NewCoalescingWorker(
		w.RedisBuffer,
		w.PostgresRepo,
		w.ProfileReg,
		w.Metrics,
		logger,
		cfg.Worker.MaxRetries,
		cfg.Ingest.FlushInterval,
	)

	// Telemetry Service (base implementation)
	baseTelemetryService := service.NewTelemetryService(w.PostgresRepo)

	// Wrap with caching decorator for chart queries
	w.TelemetryService = service.NewCachedTelemetryService(
		baseTelemetryService,
		w.ChartCache,
		logger,
		cfg.Cache.ChartTTL,
	)

	// HTTP Handler
	w.HTTPHandler = httphandler.NewHandler(
		logger,
		w.Metrics,
		w.TelemetryService,
		func() error {
			if w.MQTTSubscriber == nil || !w.MQTTSubscriber.IsConnected() {
				return fmt.Errorf("mqtt disconnected")
			}
			return nil
		},
		func() error { return w.RedisBuffer.Ping(ctx) },
		func() error { return w.PostgresRepo.Ping(ctx) },
		true,
	)

	// HTTP routes and server
	engine := httphandler.NewEngine(w.HTTPHandler, cfg.HTTP.CORSAllowedOrigins, cfg.HTTP.BasePath)
	w.HTTPServer = &http.Server{
		Addr:              cfg.HTTP.ListenAddr,
		Handler:           engine,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return w, nil
}

// StartMQTT starts the MQTT subscriber.
// MQTT always goes through IngestService -> Redis Stream (both modes).
// The worker loop selects normal batch processing or coalescing aggregation.
func (w *Wiring) StartMQTT(ctx context.Context) error {
	handler := func(payload domain.RawTelemetryPayload) error {
		w.Metrics.IncMQTTReceived()
		return w.IngestService.Handle(ctx, payload)
	}

	if err := w.MQTTSubscriber.Subscribe(ctx, handler); err != nil {
		return fmt.Errorf("mqtt subscribe: %w", err)
	}

	w.Logger.Info("mqtt subscriber started", "broker", w.Cfg.MQTT.Broker, "topic", w.Cfg.MQTT.Topic, "client_id", w.Cfg.MQTT.ClientID)
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
// It selects between normal and coalesce modes based on config.
func (w *Wiring) StartWorkerLoop(ctx context.Context) {
	if w.Cfg.Ingest.Mode == "coalesce" {
		// Coalesce mode: start flush loop + stale recovery loop.
		w.Logger.Info("starting worker in coalesce mode", "flush_interval", w.Cfg.Ingest.FlushInterval)
		go w.CoalescingWorker.StartFlushLoop(ctx)

		// Also run stale message recovery periodically (for crash recovery).
		go func() {
			interval := w.Cfg.Redis.MinIdleTime
			if interval <= 0 {
				interval = 60 * time.Second
			}

			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := w.CoalescingWorker.RecoverStale(ctx, w.Cfg.Redis.MinIdleTime, 100); err != nil {
						w.Logger.Error("coalesce recover stale failed", "error", err)
					}
				}
			}
		}()
		return
	}

	// Normal mode: use current batch processing.
	go func() {
		interval := w.Cfg.Postgres.BatchTimeout
		if interval <= 0 {
			interval = 100 * time.Millisecond
		}

		ticker := time.NewTicker(interval)
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

// StartCoalescingLoop routes messages to coalescing buffer instead of Redis Stream.
// Call this instead of directly handling in MQTT for coalesce mode.
func (w *Wiring) StartCoalescingLoop(ctx context.Context) {
	// In coalesce mode, the MQTT handler should call CoalescingWorker.Handle
	// instead of IngestService.Handle.
	// This method is a placeholder for documentation.
	// The routing happens in main.go based on config.
}

// Close closes all resources in reverse order.
func (w *Wiring) Close() error {
	w.Logger.Info("closing wiring resources")

	if w.PostgresRepo != nil {
		w.PostgresRepo.Close()
	}
	if w.RedisBuffer != nil {
		if err := w.RedisBuffer.Close(); err != nil {
			w.Logger.Warn("redis buffer close failed", "error", err)
		}
	}
	if w.ChartCache != nil {
		if err := w.ChartCache.Close(); err != nil {
			w.Logger.Warn("chart cache close failed", "error", err)
		}
	}

	return nil
}

// buildHTTPOnly creates wiring for HTTP-only role (Postgres + profiles only).
func buildHTTPOnly(ctx context.Context, cfg *config.Config, logger *logging.Logger) (*Wiring, error) {
	w := &Wiring{
		Cfg:    cfg,
		Logger: logger,
		Role:   config.RoleHTTPOnly,
	}

	w.Metrics = metrics.NewRecorder()

	// PostgreSQL Repository (required for telemetry queries)
	postgresPool, err := postgres.NewPool(ctx, cfg.Postgres, logger)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}
	w.PostgresRepo = postgres.NewRepository(postgresPool, logger)

	// Profile Registry (required for device profile lookup)
	profileReg, err := yamlprofile.NewRegistry(cfg.Profiles.Path, logger)
	if err != nil {
		postgresPool.Close()
		return nil, fmt.Errorf("profile registry: %w", err)
	}
	w.ProfileReg = profileReg

	// Try to create Redis chart cache (optional - don't panic if it fails)
	var chartCache *redisadapter.ChartCacheAdapter
	redisClient, err := redisadapter.NewClient(cfg.Redis, logger)
	if err != nil {
		logger.Warn("chart cache: Redis unavailable, running without cache", "error", err)
	} else {
		chartCache = redisadapter.NewChartCache(redisClient, logger)
		w.ChartCache = chartCache
		logger.Info("chart cache initialized")
	}

	// Telemetry Service (base implementation)
	baseTelemetryService := service.NewTelemetryService(w.PostgresRepo)

	// Wrap with caching decorator if cache is available
	if chartCache != nil {
		w.TelemetryService = service.NewCachedTelemetryService(
			baseTelemetryService,
			chartCache,
			logger,
			cfg.Cache.ChartTTL,
		)
	} else {
		w.TelemetryService = baseTelemetryService
	}

	// HTTP Handler (nil pings since no MQTT/Redis)
	w.HTTPHandler = httphandler.NewHandler(
		logger,
		w.Metrics,
		w.TelemetryService,
		nil, // mqttPing - not available in http-only mode
		nil, // redisPing - not available in http-only mode
		func() error { return w.PostgresRepo.Ping(ctx) },
		true,
	)

	engine := httphandler.NewEngine(w.HTTPHandler, cfg.HTTP.CORSAllowedOrigins, cfg.HTTP.BasePath)
	w.HTTPServer = &http.Server{
		Addr:              cfg.HTTP.ListenAddr,
		Handler:           engine,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return w, nil
}
