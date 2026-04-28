package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

// StreamBuffer implements MessageBuffer using Redis Streams.
type StreamBuffer struct {
	client           *redis.Client
	stream           string
	deadletterStream string
	group            string
	consumer         string
	blockTime        time.Duration
	logger           *logging.Logger
}

// Config holds Redis connection configuration.
type Config struct {
	Addr             string
	Password         string
	DB               int
	Stream           string
	DeadletterStream string
	Group            string
	Consumer         string
	BlockTime        time.Duration
}

// NewStreamBuffer creates a new Redis Stream buffer.
func NewStreamBuffer(cfg Config, logger *logging.Logger) (*StreamBuffer, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Ping to verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connect failed: %w", err)
	}

	// Create consumer group if it doesn't exist
	err := client.XGroupCreateMkStream(ctx, cfg.Stream, cfg.Group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		logger.Warn("redis group creation", "error", err)
	}

	return &StreamBuffer{
		client:           client,
		stream:           cfg.Stream,
		deadletterStream: cfg.DeadletterStream,
		group:            cfg.Group,
		consumer:         cfg.Consumer,
		logger:           logger,
	}, nil
}

// Add writes a message to the stream.
func (s *StreamBuffer) Add(ctx context.Context, msg domain.RawTelemetryMessage) error {
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	fields := map[string]interface{}{
		"device_id":   msg.Payload.DeviceID,
		"payload":     string(payloadBytes),
		"received_at": msg.ReceivedAt.Format(time.RFC3339Nano),
	}

	id, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.stream,
		Values: fields,
	}).Result()

	if err != nil {
		return fmt.Errorf("xadd failed: %w", err)
	}

	s.logger.Debug("redis xadd", "id", id, "device_id", msg.Payload.DeviceID)
	return nil
}

// ReadBatch reads a batch of messages from the stream.
func (s *StreamBuffer) ReadBatch(ctx context.Context, max int) ([]domain.BufferedMessage, error) {
	// Use XREADGROUP to read new messages
	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.group,
		Consumer: s.consumer,
		Streams:  []string{s.stream, ">"},
		Count:    int64(max),
		Block:    0, // blocking
	}).Result()

	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("xreadgroup failed: %w", err)
	}

	var messages []domain.BufferedMessage
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			parsed, err := s.parseMessage(msg)
			if err != nil {
				s.logger.Error("parse message error", "id", msg.ID, "error", err)
				continue
			}
			messages = append(messages, parsed)
		}
	}

	s.logger.Debug("redis read batch", "count", len(messages))
	return messages, nil
}

// parseMessage converts a Redis message to BufferedMessage.
func (s *StreamBuffer) parseMessage(msg redis.XMessage) (domain.BufferedMessage, error) {
	payloadStr, ok := msg.Values["payload"].(string)
	if !ok {
		return domain.BufferedMessage{}, fmt.Errorf("missing payload field")
	}

	var payload domain.RawTelemetryPayload
	if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
		return domain.BufferedMessage{}, fmt.Errorf("unmarshal payload: %w", err)
	}

	return domain.BufferedMessage{
		ID:      msg.ID,
		Payload: payload,
	}, nil
}

// Ack acknowledges processed messages.
func (s *StreamBuffer) Ack(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	err := s.client.XAck(ctx, s.stream, s.group, ids...).Err()
	if err != nil {
		return fmt.Errorf("xack failed: %w", err)
	}

	s.logger.Debug("redis ack", "count", len(ids))
	return nil
}

// ClaimStale claims messages that have been pending too long.
func (s *StreamBuffer) ClaimStale(ctx context.Context, minIdle time.Duration, max int) ([]domain.BufferedMessage, error) {
	// Use XAUTOCLAIM to claim stale messages
	claims, _, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s.stream,
		Group:    s.group,
		Consumer: s.consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    int64(max),
	}).Result()

	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("xautoclaim failed: %w", err)
	}

	var messages []domain.BufferedMessage
	for _, msg := range claims {
		parsed, err := s.parseMessage(msg)
		if err != nil {
			s.logger.Error("claim parse error", "id", msg.ID, "error", err)
			continue
		}
		messages = append(messages, parsed)
	}

	s.logger.Debug("redis claim stale", "count", len(messages))
	return messages, nil
}

// Deadletter moves a failed message to the deadletter stream.
func (s *StreamBuffer) Deadletter(ctx context.Context, msg domain.BufferedMessage, reason string) error {
	failedAt := time.Now().UTC().Format(time.RFC3339Nano)
	payloadBytes, _ := json.Marshal(msg.Payload)

	fields := map[string]interface{}{
		"redis_stream_id":  msg.ID,
		"failed_at":        failedAt,
		"error":            reason,
		"original_payload": string(payloadBytes),
	}

	_, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.deadletterStream,
		Values: fields,
	}).Result()

	if err != nil {
		return fmt.Errorf("xadd deadletter failed: %w", err)
	}

	s.logger.Info("message deadlettered", "id", msg.ID, "reason", reason)
	return nil
}

// Length returns the number of messages in the stream.
func (s *StreamBuffer) Length(ctx context.Context) (int64, error) {
	length, err := s.client.XLen(ctx, s.stream).Result()
	if err != nil {
		return 0, fmt.Errorf("xlen failed: %w", err)
	}
	return length, nil
}

// Close closes the Redis client.
func (s *StreamBuffer) Close() error {
	return s.client.Close()
}

// Ping checks Redis connectivity.
func (s *StreamBuffer) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}
