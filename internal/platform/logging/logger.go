package logging

import (
	"context"
	"log/slog"
	"os"
	"time"
)

// Logger wraps slog for structured logging.
type Logger struct {
	*slog.Logger
}

// New creates a new logger with the specified level.
func New(level string) *Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: lvl,
	})

	return &Logger{slog.New(handler)}
}

// With returns a new logger with the given attributes.
func (l *Logger) With(attrs ...any) *Logger {
	return &Logger{l.Logger.With(attrs...)}
}

// Context returns a logger with context attached.
func (l *Logger) Context(ctx context.Context) context.Context {
	return ContextWithLogger(ctx, l)
}

// ContextWithLogger adds logger to context.
func ContextWithLogger(ctx context.Context, logger *Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// FromContext retrieves logger from context.
func FromContext(ctx context.Context) *Logger {
	if logger, ok := ctx.Value(loggerKey{}).(*Logger); ok {
		return logger
	}
	return New("info")
}

type loggerKey struct{}

// Default logger instance.
var defaultLogger = New("info")

// Info logs at info level.
func Info(msg string, attrs ...any) {
	defaultLogger.Info(msg, attrs...)
}

// Error logs at error level.
func Error(msg string, attrs ...any) {
	defaultLogger.Error(msg, attrs...)
}

// Debug logs at debug level.
func Debug(msg string, attrs ...any) {
	defaultLogger.Debug(msg, attrs...)
}

// Warn logs at warn level.
func Warn(msg string, attrs ...any) {
	defaultLogger.Warn(msg, attrs...)
}

// WithTime returns current time for timing operations.
func WithTime() slog.Attr {
	return slog.Time("time", time.Now().UTC())
}
