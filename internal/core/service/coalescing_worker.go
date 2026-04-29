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
// It reads from Redis Stream, keeps latest per device_id|profile_id, and flushes periodically.
// Uses Redis Stream for durability - messages are acked only after successful DB insert.
type CoalescingWorker struct {
	buffer     ports.MessageBuffer
	repo       ports.TelemetryRepository
	profiles   ports.ProfileRegistry
	metrics    ports.MetricsRecorder
	logger     *logging.Logger
	maxRetries int

	flushInterval time.Duration

	mu      sync.Mutex
	pending map[string]*coalescedEntry
}

// coalescedEntry holds the latest message and all stream IDs for a given key.
type coalescedEntry struct {
	key       string
	msg       domain.BufferedMessage
	timestamp time.Time
	streamIDs []string // all stream IDs seen for this key
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

// StartFlushLoop reads from Redis Stream and periodically flushes.
// Call this in a goroutine.
func (s *CoalescingWorker) StartFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.flush(ctx)
			return
		case <-ticker.C:
			s.readAndFlush(ctx)
		}
	}
}

// readAndFlush reads a batch from Redis Stream and flushes latest per key.
func (s *CoalescingWorker) readAndFlush(ctx context.Context) {
	// Read batch from Redis Stream.
	messages, err := s.buffer.ReadBatch(ctx, 1000)
	if err != nil {
		s.logger.Error("coalescing: read failed", "error", err)
		return
	}

	// Always flush after every tick - even if no new messages arrived.
	// This ensures recovered stale entries get flushed during idle periods.
	if len(messages) > 0 {
		s.logger.Debug("coalescing: batch read", "count", len(messages))

		s.mu.Lock()
		for _, msg := range messages {
			if err := s.bufferOneLocked(msg); err != nil {
				s.logger.Error("coalescing: buffer one failed", "id", msg.ID, "error", err)
			}
		}
		entryCount := len(s.pending)
		s.mu.Unlock()

		s.logger.Debug("coalescing: buffered", "entries", entryCount)
	}

	// Flush now - runs every tick to drain any pending work (including recovered stale entries).
	s.flush(ctx)
}

// bufferOne adds a single message to the coalescing buffer.
// Thread-safe - acquires lock before mutating pending.
func (s *CoalescingWorker) bufferOne(msg domain.BufferedMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bufferOneLocked(msg)
}

// bufferOneLocked adds a single message to the coalescing buffer.
// Assumes caller holds s.mu. Use for batch operations where lock is already held.
func (s *CoalescingWorker) bufferOneLocked(msg domain.BufferedMessage) error {
	payload := msg.Payload

	normalizedTime, err := domain.NormalizeTimestamp(payload)
	if err != nil {
		return err
	}

	profile, err := s.profiles.Match(payload)
	if err != nil {
		return err
	}

	profileID := "unknown"
	if profile != nil {
		profileID = profile.ID
	}

	key := fmt.Sprintf("%s|%s", payload.DeviceID, profileID)

	existing, exists := s.pending[key]
	if exists && !normalizedTime.After(existing.timestamp) {
		// Keep existing - add stream ID to ack list.
		existing.streamIDs = append(existing.streamIDs, msg.ID)
		return nil
	}

	var oldIDs []string
	if exists {
		// Replacing with newer - keep old stream IDs too.
		oldIDs = existing.streamIDs
	}

	// Replace with newer - include any old IDs in the ack list.
	s.pending[key] = &coalescedEntry{
		key:       key,
		msg:       msg,
		timestamp: normalizedTime,
		streamIDs: append(oldIDs, msg.ID),
	}

	return nil
}

// BufferOne adds a single message to the coalescing buffer.
// Exported for testing.
func (s *CoalescingWorker) BufferOne(msg domain.BufferedMessage) error {
	return s.bufferOne(msg)
}

// flush writes all pending entries to the database.
// Only clears Redis IDs after successful insert.
func (s *CoalescingWorker) flush(ctx context.Context) {
	// Shutdown callers may pass a canceled context; use a fresh timeout so
	// the final coalesced snapshot can still be persisted and acked.
	if ctx.Err() != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		ctx = shutdownCtx
	}

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
	var allStreamIDs []string
	var failedEntries []*coalescedEntry

	for _, e := range entries {
		enriched, err := s.processMessage(ctx, e.msg)
		if err != nil {
			s.logger.Error("coalescing: process failed", "key", e.key, "error", err)
			s.metrics.IncWorkerFailed(err.Error())
			failedEntries = append(failedEntries, e)
			continue
		}

		rows = append(rows, *enriched)
		allStreamIDs = append(allStreamIDs, e.streamIDs...)
	}

	// Handle failed entries - retry or deadletter.
	s.handleFailedEntries(ctx, failedEntries)

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

	// Only ack after successful insert.
	if len(allStreamIDs) > 0 {
		if err := s.buffer.Ack(ctx, allStreamIDs); err != nil {
			s.logger.Error("coalescing: ack failed", "error", err)
		}
		if err := s.buffer.ResetRetry(ctx, allStreamIDs); err != nil {
			s.logger.Error("coalescing: reset retry failed", "error", err)
		}
	}
}

// handleFailedEntries processes failed entries - increments retry count and deadletters if max retries reached.
func (s *CoalescingWorker) handleFailedEntries(ctx context.Context, failedEntries []*coalescedEntry) {
	if len(failedEntries) == 0 {
		return
	}

	for _, e := range failedEntries {
		// Increment retry counter for all stream IDs in this entry.
		var maxRetryCount int
		for _, id := range e.streamIDs {
			retryCount, err := s.buffer.IncrementRetry(ctx, id)
			if err != nil {
				s.logger.Error("coalescing: increment retry failed", "id", id, "error", err)
				continue
			}
			if retryCount > maxRetryCount {
				maxRetryCount = retryCount
			}
		}

		// If max retries reached, deadletter the entry.
		if maxRetryCount >= s.maxRetries {
			reason := fmt.Sprintf("max retries exceeded after %d attempts", maxRetryCount)
			if err := s.buffer.Deadletter(ctx, e.msg, reason, maxRetryCount); err != nil {
				s.logger.Error("coalescing: deadletter failed", "key", e.key, "error", err)
				continue
			}

			s.metrics.IncDeadlettered()
			s.logger.Warn("coalescing: deadlettered entry", "key", e.key, "retries", maxRetryCount)

			if err := s.buffer.Ack(ctx, e.streamIDs); err != nil {
				s.logger.Error("coalescing: deadletter ack failed", "key", e.key, "error", err)
			}
			if err := s.buffer.ResetRetry(ctx, e.streamIDs); err != nil {
				s.logger.Error("coalescing: deadletter reset retry failed", "key", e.key, "error", err)
			}
		}
	}
}

// RecoverStale claims stale messages and re-buffers them.
// For crash recovery - orphaned Redis entries are reclaimed and coalesced.
func (s *CoalescingWorker) RecoverStale(ctx context.Context, minIdle time.Duration, maxCount int) error {
	messages, err := s.buffer.ClaimStale(ctx, minIdle, maxCount)
	if err != nil {
		s.logger.Error("coalescing: claim stale failed", "error", err)
		return err
	}

	if len(messages) == 0 {
		return nil
	}

	s.logger.Info("coalescing: recovering stale", "count", len(messages))

	// Buffer all reclaimed messages (latest per key will win).
	for _, msg := range messages {
		if err := s.bufferOne(msg); err != nil {
			s.logger.Error("coalescing: buffer stale failed", "id", msg.ID, "error", err)
		}
	}

	return nil
}

// processMessage transforms a buffered message into enriched telemetry.
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
