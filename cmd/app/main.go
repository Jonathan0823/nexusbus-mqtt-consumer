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
	logger.Info("starting telemetry service", "role", cfg.Service.Role)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create app
	app := runtime.NewApp(cfg, logger)

	// Create wiring (role-based)
	wiring, err := runtime.NewWiring(ctx, cfg, logger)
	if err != nil {
		logger.Error("wiring creation failed", "error", err)
		os.Exit(1)
	}

	// Add wiring closer to app
	app.AddComponentFunc("wiring", wiring.Close)

	// Role-based startup
	switch cfg.Service.Role {
	case config.RoleHTTPOnly:
		// HTTP-only mode: only start HTTP server
		if err := wiring.StartHTTPServer(); err != nil {
			logger.Error("http server start failed", "error", err)
			os.Exit(1)
		}

	case config.RoleAll, config.RoleIngestOnly, config.RoleWorkerOnly:
		// Full stack or specific roles
		// Add coalescer flush before wiring close (runs first in coalesce mode)
		if cfg.Ingest.Mode == "coalesce" {
			app.AddComponentFunc("coalescer-flush", func() error {
				logger.Info("flushing coalesced buffer before shutdown")
				// Use a fresh context for shutdown flush
				flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if wiring.CoalescingWorker != nil {
					return wiring.CoalescingWorker.Flush(flushCtx)
				}
				return nil
			})
		}

		// Add MQTT subscriber closer
		if wiring.MQTTSubscriber != nil {
			app.AddComponentFunc("mqtt", func() error {
				return wiring.MQTTSubscriber.Close()
			})
		}

		// Start MQTT subscriber
		if wiring.MQTTSubscriber != nil {
			if err := wiring.StartMQTT(ctx); err != nil {
				logger.Error("mqtt start failed", "error", err)
				os.Exit(1)
			}
		}

		// Start HTTP server
		if wiring.HTTPServer != nil {
			if err := wiring.StartHTTPServer(); err != nil {
				logger.Error("http server start failed", "error", err)
				os.Exit(1)
			}
		}

		// Start worker loop
		if wiring.WorkerService != nil {
			wiring.StartWorkerLoop(ctx)
		}

	default:
		logger.Error("unknown service role", "role", cfg.Service.Role)
		os.Exit(1)
	}

	// Add HTTP server shutdown if available
	if wiring.HTTPServer != nil {
		app.AddComponentFunc("http-server", func() error {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return wiring.HTTPServer.Shutdown(shutdownCtx)
		})
	}

	// Run app - blocks until shutdown
	if err := app.Run(ctx); err != nil {
		logger.Error("app runtime error", "error", err)
	}

	logger.Info("service stopped")
}
