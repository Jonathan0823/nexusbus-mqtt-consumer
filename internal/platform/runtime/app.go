package runtime

import (
	"context"
	"time"

	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"
	"modbus-mqtt-consumer/internal/platform/shutdown"
)

// App manages the application lifecycle.
type App struct {
	cfg    *config.Config
	logger *logging.Logger

	// Shutdown manager
	shutdownMgr *shutdown.Manager

	// Components
	components []shutdown.Component
}

// NewApp creates a new application instance.
func NewApp(cfg *config.Config, logger *logging.Logger) *App {
	return &App{
		cfg:    cfg,
		logger: logger,
	}
}

// AddComponent adds a component to the app lifecycle.
func (a *App) AddComponent(c shutdown.Component) {
	a.components = append(a.components, c)
}

// AddComponentFunc adds a component by name and closer function.
func (a *App) AddComponentFunc(name string, closer func() error) {
	a.AddComponent(&fnComponent{name: name, closeFn: closer})
}

type fnComponent struct {
	name    string
	closeFn func() error
}

func (c *fnComponent) Close() error {
	return c.closeFn()
}

func (c *fnComponent) Name() string {
	return c.name
}

// Run starts the application and blocks until shutdown.
func (a *App) Run(ctx context.Context) error {
	// Initialize shutdown manager
	a.shutdownMgr = shutdown.NewManager(a.logger, 30*time.Second)
	for _, c := range a.components {
		a.shutdownMgr.Add(c)
	}

	// Run shutdown manager in background
	runCtx := a.shutdownMgr.Run(ctx)
	<-runCtx.Done()
	a.shutdownMgr.Wait()

	a.logger.Info("application stopped")
	return nil
}

// ShutdownManager returns the shutdown manager for external use.
func (a *App) ShutdownManager() *shutdown.Manager {
	return a.shutdownMgr
}
