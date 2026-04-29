package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/ports"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// CoalescingWorker handles telemetry with in-memory coalescing.
// It keeps only the latest message per device_id|profile_id and flushes periodically.
type CoalescingWorker struct {
	buffer     ports.MessageBuffer
	repo       ports.TelemetryRepository
	profiles   ports.ProfileRegistry
	metrics    ports.MetricsRecorder
	logger     *logging.Logger
	maxRetries int

	// flushInterval controls how often to flush to database.
	flushInterval time.Duration

	// mu protects the buffer map.
	mu sync.Mutex
	// pending maps device_id|profile_id to buffered entry.
	pending map[string]*coalescedEntry
}

// coalescedEntry holds the latest message for a given key.
type coalescedEntry struct {
	key       string
	msg       domain.BufferedMessage
	timestamp time.Time
}

// NewCoalescingWorker creates a new coalescing worker service.
func NewCoalescingWorker(
	buffer ports.MessageBuffer,
	repo ports.TelemetryRepository,
	profiles ports.ProfileRegistry,
	metrics ports.MetricsRecorder,
	logger *logging.Logger,
	maxRetries int,
	flushInterval time.Duration,
) *CoalescingWorker {
	return &CoalescingWorker{
		buffer:        buffer,
		repo:          repo,
		profiles:      profiles,
		metrics:       metrics,
		logger:        logger,
		maxRetries:    maxRetries,
		flushInterval: flushInterval,
		pending:       make(map[string]*coalescedEntry),
	}
}

// Handle processes an incoming message into the coalescing buffer.
func (s *CoalescingWorker) Handle(ctx context.Context, msg domain.BufferedMessage) error {
	payload := msg.Payload

	// Normalize timestamp
	normalizedTime, err := domain.NormalizeTimestamp(payload)
	if err != nil {
		s.logger.Error("normalize timestamp failed", "error", err, "device_id", payload.DeviceID)
		return err
	}

	// Match profile to get profile_id for key
	profile, err := s.profiles.Match(payload)
	if err != nil {
		return err
	}

	profileID := "unknown"
	if profile != nil {
		profileID = profile.ID
	}

	// Build the coalescing key: device_id|profile_id
	key := fmt.Sprintf("%s|%s", payload.DeviceID, profileID)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we should replace the existing entry.
	existing, exists := s.pending[key]
	if exists && !normalizedTime.After(existing.timestamp) {
		// Existing timestamp is newer or equal - keep the old one.
		s.logger.Debug("coalescing: keeping existing", "key", key, "existing_time", existing.timestamp, "new_time", normalizedTime)
		return nil
	}

	// Replace with newer message.
	s.pending[key] = &coalescedEntry{
		key:       key,
		msg:       msg,
		timestamp: normalizedTime,
	}

	s.logger.Debug("coalescing: buffered", "key", key, "timestamp", normalizedTime)
	return nil
}

// StartFlushLoop starts the periodic flush loop.
// Call this in a goroutine.
func (s *CoalescingWorker) StartFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush any remaining on shutdown.
			s.flush(ctx)
			return
		case <-ticker.C:
			s.flush(ctx)
		}
	}
}

// flush writes all pending entries to the database.
func (s *CoalescingWorker) flush(ctx context.Context) {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return
	}

	// Snapshot current pending and clear the map.
	entries := make([]*coalescedEntry, 0, len(s.pending))
	for _, e := range s.pending {
		entries = append(entries, e)
	}
	s.pending = make(map[string]*coalescedEntry)
	s.mu.Unlock()

	s.logger.Info("coalescing flush: processing", "count", len(entries))

	// Process each entry.
	var rows []domain.EnrichedTelemetry
	var streamIDs []string

	for _, e := range entries {
		enriched, err := s.processMessage(ctx, e.msg)
		if err != nil {
			s.logger.Error("coalescing: process failed", "key", e.key, "error", err)
			s.metrics.IncWorkerFailed(err.Error())
			continue
		}

		rows = append(rows, *enriched)
		streamIDs = append(streamIDs, e.msg.ID)
	}

	// Batch insert.
	if len(rows) > 0 {
		inserted, err := s.repo.InsertBatchIdempotent(ctx, rows)
		if err != nil {
			s.logger.Error("coalescing: batch insert failed", "error", err)
			return
		}

		s.metrics.IncWorkerProcessed()
		s.logger.Info("coalescing: inserted", "total", len(rows), "inserted", inserted)
	}

	// Ack all stream IDs after successful insert.
	if len(streamIDs) > 0 {
		if err := s.buffer.Ack(ctx, streamIDs); err != nil {
			s.logger.Error("coalescing: ack failed", "error", err)
		}
		if err := s.buffer.ResetRetry(ctx, streamIDs); err != nil {
			s.logger.Error("coalescing: reset retry failed", "error", err)
		}
	}
}

// processMessage transforms a buffered message into enriched telemetry.
// This is copied from WorkerService for independence.
func (s *CoalescingWorker) processMessage(ctx context.Context, msg domain.BufferedMessage) (*domain.EnrichedTelemetry, error) {
	payload := msg.Payload

	normalizedTime, err := domain.NormalizeTimestamp(payload)
	if err != nil {
		return nil, err
	}

	profile, err := s.profiles.Match(payload)
	if err != nil {
		return nil, err
	}

	profileID := "unknown"
	if profile != nil {
		profileID = profile.ID
	}

	metrics, err := s.profiles.Transform(payload, profile)
	if err != nil {
		return nil, err
	}

	idempotencyKey := domain.BuildIdempotencyKey(payload, normalizedTime)

	var rawPayload *json.RawMessage
	if profile == nil {
		rawBytes, _ := json.Marshal(payload)
		raw := json.RawMessage(rawBytes)
		rawPayload = &raw
	}

	enriched := &domain.EnrichedTelemetry{
		Time:           normalizedTime,
		ReceivedAt:     time.Now().UTC(),
		DeviceID:       payload.DeviceID,
		ProfileID:      profileID,
		RegisterType:   payload.RegisterType,
		Address:        payload.Address,
		Count:          payload.Count,
		Source:         payload.Source,
		IdempotencyKey: idempotencyKey,
		Metrics:        metrics,
		RawPayload:     rawPayload,
	}

	return enriched, nil
}

// Flush forces an immediate flush of pending entries.
// Use on shutdown.
func (s *CoalescingWorker) Flush(ctx context.Context) error {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	s.flush(ctx)
	return nil
}

// PendingCount returns the number of pending entries.
// For metrics/debugging.
func (s *CoalescingWorker) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}
