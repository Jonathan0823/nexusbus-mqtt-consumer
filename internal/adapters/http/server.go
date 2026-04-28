package httpadapter

import (
	"context"
	"fmt"
	stdhttp "net/http"
	"time"

	"modbus-mqtt-consumer/internal/platform/logging"
)

// Server wraps http.Server with lifecycle management.
type Server struct {
	server *stdhttp.Server
	logger *logging.Logger
}

// NewServer creates a new HTTP server wrapper.
// This follows the design.md adapter creation pattern.
func NewServer(addr string, handler stdhttp.Handler, logger *logging.Logger) *Server {
	return &Server{
		server: &stdhttp.Server{
			Addr:    addr,
			Handler: handler,
		},
		logger: logger,
	}
}

// Start runs the HTTP server.
func (s *Server) Start() error {
	s.logger.Info("http server starting", "addr", s.server.Addr)
	if err := s.server.ListenAndServe(); err != nil && err != stdhttp.ErrServerClosed {
		return fmt.Errorf("http listen and serve: %w", err)
	}
	return nil
}

// Close shuts down the HTTP server gracefully.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// Name returns the component name for shutdown.Manager.
func (s *Server) Name() string {
	return "http-server"
}
