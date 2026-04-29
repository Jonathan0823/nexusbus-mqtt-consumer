package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/runtime"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		os.Stderr.WriteString("config load error: " + err.Error() + "\n")
		os.Exit(1)
	}

	logger := logging.New(cfg.Service.LogLevel)
	logger.Info("starting telemetry service")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create app
	app := runtime.NewApp(cfg, logger)

	// Create wiring
	wiring, err := runtime.NewWiring(ctx, cfg, logger)
	if err != nil {
		logger.Error("wiring creation failed", "error", err)
		os.Exit(1)
	}

	// Add wiring closer to app
	app.AddComponentFunc("wiring", wiring.Close)

	// Add coalescer flush before wiring close (runs first in coalesce mode)
	if cfg.Ingest.Mode == "coalesce" {
		app.AddComponentFunc("coalescer-flush", func() error {
			logger.Info("flushing coalesced buffer before shutdown")
			return wiring.CoalescingWorker.Flush(ctx)
		})
	}

	// Add MQTT subscriber
	app.AddComponentFunc("mqtt", func() error {
		return wiring.MQTTSubscriber.Close()
	})

	// Add HTTP server
	app.AddComponentFunc("http-server", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return wiring.HTTPServer.Shutdown(ctx)
	})

	// Start MQTT subscriber
	if err := wiring.StartMQTT(ctx); err != nil {
		logger.Error("mqtt start failed", "error", err)
		os.Exit(1)
	}

	// Start HTTP server
	if err := wiring.StartHTTPServer(); err != nil {
		logger.Error("http server start failed", "error", err)
		os.Exit(1)
	}

	// Start worker loop
	wiring.StartWorkerLoop(ctx)

	// Run app - blocks until shutdown
	if err := app.Run(ctx); err != nil {
		logger.Error("app runtime error", "error", err)
	}

	logger.Info("service stopped")
}
