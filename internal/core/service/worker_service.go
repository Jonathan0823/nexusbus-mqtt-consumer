package service

import (
	"context"
	"encoding/json"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/core/ports"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// WorkerService handles telemetry enrichment and persistence.
type WorkerService struct {
	buffer     ports.MessageBuffer
	repo       ports.TelemetryRepository
	profiles   ports.ProfileRegistry
	metrics    ports.MetricsRecorder
	logger     *logging.Logger
	maxRetries int
}

// NewWorkerService creates a new worker service.
func NewWorkerService(
	buffer ports.MessageBuffer,
	repo ports.TelemetryRepository,
	profiles ports.ProfileRegistry,
	metrics ports.MetricsRecorder,
	logger *logging.Logger,
	maxRetries int,
) *WorkerService {
	return &WorkerService{
		buffer:     buffer,
		repo:       repo,
		profiles:   profiles,
		metrics:    metrics,
		logger:     logger,
		maxRetries: maxRetries,
	}
}

// ProcessBatch reads and processes a batch of messages from Redis.
func (s *WorkerService) ProcessBatch(ctx context.Context, maxCount int) error {
	// Read batch from Redis
	messages, err := s.buffer.ReadBatch(ctx, maxCount)
	if err != nil {
		s.logger.Error("read batch failed", "error", err)
		return err
	}

	if len(messages) == 0 {
		return nil
	}

	// Process each message and collect for batch insert
	var rows []domain.EnrichedTelemetry
	var idsToAck []string

	for _, msg := range messages {
		enriched, err := s.processMessage(ctx, msg)
		if err != nil {
			s.logger.Error("process message failed", "id", msg.ID, "error", err)
			s.metrics.IncWorkerFailed(err.Error())
			if shouldAck, retryErr := s.handleFailure(ctx, msg, err.Error()); retryErr != nil {
				s.logger.Error("retry handling failed", "id", msg.ID, "error", retryErr)
			} else if shouldAck {
				idsToAck = append(idsToAck, msg.ID)
			}
			continue
		}

		rows = append(rows, *enriched)
		idsToAck = append(idsToAck, msg.ID)
	}

	// Batch insert and commit before acknowledging Redis messages.
	if len(rows) > 0 {
		inserted, err := s.persistBatch(ctx, rows)
		if err != nil {
			s.logger.Error("batch insert failed", "error", err)
			return err
		}

		s.metrics.IncWorkerProcessed()
		for i := 0; i < len(rows)-inserted; i++ {
			s.metrics.IncWorkerDuplicate()
		}
		s.logger.Debug("batch processed", "total", len(rows), "inserted", inserted)
	}

	// Ack only after the batch commit (or duplicate skip) succeeds.
	if len(idsToAck) > 0 {
		if err := s.ackMessages(ctx, idsToAck); err != nil {
			s.logger.Error("ack failed", "error", err)
			return err
		}
		if err := s.buffer.ResetRetry(ctx, idsToAck); err != nil {
			s.logger.Error("reset retry failed", "error", err)
		}
	}

	return nil
}

// processMessage transforms a buffered message into enriched telemetry.
func (s *WorkerService) processMessage(ctx context.Context, msg domain.BufferedMessage) (*domain.EnrichedTelemetry, error) {
	payload := msg.Payload

	// Normalize timestamp
	normalizedTime, err := domain.NormalizeTimestamp(payload)
	if err != nil {
		return nil, err
	}

	// Match profile
	profile, err := s.profiles.Match(payload)
	if err != nil {
		return nil, err
	}

	profileID := "unknown"
	if profile != nil {
		profileID = profile.ID
	}

	// Transform values to metrics
	metrics, err := s.profiles.Transform(payload, profile)
	if err != nil {
		return nil, err
	}

	// Build idempotency key (prefers message_id when present)
	idempotencyKey := domain.BuildIdempotencyKey(payload, normalizedTime)

	// Determine if we should store raw payload
	var rawPayload *json.RawMessage
	if profile == nil {
		// Store raw for unknown profiles
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

// persistBatch writes enriched telemetry to PostgreSQL.
func (s *WorkerService) persistBatch(ctx context.Context, rows []domain.EnrichedTelemetry) (int, error) {
	return s.repo.InsertBatchIdempotent(ctx, rows)
}

// ackMessages acknowledges messages after persistence has completed.
func (s *WorkerService) ackMessages(ctx context.Context, ids []string) error {
	return s.buffer.Ack(ctx, ids)
}

// handleFailure increments retry state and deadletters when limit is reached.
func (s *WorkerService) handleFailure(ctx context.Context, msg domain.BufferedMessage, reason string) (bool, error) {
	retryCount, err := s.buffer.IncrementRetry(ctx, msg.ID)
	if err != nil {
		return false, err
	}

	if retryCount < s.maxRetries {
		return false, nil
	}

	if err := s.buffer.Deadletter(ctx, msg, reason, retryCount); err != nil {
		return false, err
	}

	s.metrics.IncDeadlettered()
	return true, nil
}

// RecoverStale claims and reprocesses stale pending messages.
func (s *WorkerService) RecoverStale(ctx context.Context, minIdle time.Duration, maxCount int) error {
	messages, err := s.buffer.ClaimStale(ctx, minIdle, maxCount)
	if err != nil {
		s.logger.Error("claim stale failed", "error", err)
		return err
	}

	if len(messages) == 0 {
		return nil
	}

	s.logger.Info("recovering stale messages", "count", len(messages))

	// Process each stale message
	var idsToAck []string

	for _, msg := range messages {
		enriched, err := s.processMessage(ctx, msg)
		if err != nil {
			s.logger.Error("recover stale message failed", "id", msg.ID, "error", err)
			if shouldAck, retryErr := s.handleFailure(ctx, msg, err.Error()); retryErr != nil {
				s.logger.Error("retry handling failed", "id", msg.ID, "error", retryErr)
			} else if shouldAck {
				idsToAck = append(idsToAck, msg.ID)
			}
			continue
		}

		_, err = s.repo.InsertBatchIdempotent(ctx, []domain.EnrichedTelemetry{*enriched})
		if err != nil {
			s.logger.Error("recover insert failed", "id", msg.ID, "error", err)
			if shouldAck, retryErr := s.handleFailure(ctx, msg, err.Error()); retryErr != nil {
				s.logger.Error("retry handling failed", "id", msg.ID, "error", retryErr)
			} else if shouldAck {
				idsToAck = append(idsToAck, msg.ID)
			}
			continue
		}

		idsToAck = append(idsToAck, msg.ID)
	}

	if len(idsToAck) > 0 {
		if err := s.buffer.Ack(ctx, idsToAck); err != nil {
			s.logger.Error("recover ack failed", "error", err)
			return err
		}
		if err := s.buffer.ResetRetry(ctx, idsToAck); err != nil {
			s.logger.Error("recover reset retry failed", "error", err)
		}
	}

	return nil
}
