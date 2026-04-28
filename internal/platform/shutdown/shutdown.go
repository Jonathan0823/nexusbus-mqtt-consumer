package shutdown

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"modbus-mqtt-consumer/internal/platform/logging"
)

// Manager handles graceful shutdown.
type Manager struct {
	logger      *logging.Logger
	signals     []os.Signal
	waitTimeout time.Duration
	done        chan struct{}
	components  []Component
	once        sync.Once
}

// Component is a component that can be shut down.
type Component interface {
	// Close gracefully shuts down the component.
	Close() error
	// Name returns the component name.
	Name() string
}

// NewManager creates a new shutdown manager.
func NewManager(logger *logging.Logger, waitTimeout time.Duration) *Manager {
	return &Manager{
		logger:      logger,
		signals:     []os.Signal{syscall.SIGINT, syscall.SIGTERM},
		waitTimeout: waitTimeout,
		done:        make(chan struct{}),
	}
}

// Add adds a component to be shut down.
func (m *Manager) Add(c Component) {
	m.components = append(m.components, c)
}

// Run starts the shutdown manager in a goroutine.
// It returns a context that is cancelled on shutdown signal.
func (m *Manager) Run(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		sigChan := make(chan os.Signal, len(m.signals))
		signal.Notify(sigChan, m.signals...)
		defer signal.Stop(sigChan)

		select {
		case sig := <-sigChan:
			m.logger.Info("shutdown signal received", "signal", sig.String())
			cancel()
			m.shutdown()
		case <-ctx.Done():
			m.shutdown()
		}
	}()

	return ctx
}

// shutdown performs graceful shutdown of all components.
func (m *Manager) shutdown() {
	m.once.Do(func() {
		m.logger.Info("starting graceful shutdown", "components", len(m.components))

		for i := len(m.components) - 1; i >= 0; i-- {
			c := m.components[i]
			m.logger.Info("stopping component", "name", c.Name())
			if err := c.Close(); err != nil {
				m.logger.Error("component close error", "name", c.Name(), "error", err)
			}
		}

		m.logger.Info("graceful shutdown complete")
		close(m.done)
	})
}

// Done returns a channel that is closed when shutdown is complete.
func (m *Manager) Done() <-chan struct{} {
	return m.done
}

// Wait blocks until shutdown is complete.
func (m *Manager) Wait() {
	if m.waitTimeout <= 0 {
		<-m.done
		return
	}

	select {
	case <-m.done:
	case <-time.After(m.waitTimeout):
		m.logger.Warn("shutdown wait timed out")
	}
}
