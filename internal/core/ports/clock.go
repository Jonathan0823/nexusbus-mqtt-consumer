package ports

import (
	"time"
)

// Clock defines the interface for time operations.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// Since returns the duration since the given time.
	Since(time.Time) time.Duration
}