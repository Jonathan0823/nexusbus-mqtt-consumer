package service

import (
	"context"
	"testing"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

type fakeBuffer struct {
	readBatch []domain.BufferedMessage
	ackIDs    []string
	resetIDs  []string
}

func (f *fakeBuffer) Add(context.Context, domain.RawTelemetryMessage) error { return nil }
func (f *fakeBuffer) ReadBatch(context.Context, int) ([]domain.BufferedMessage, error) {
	return append([]domain.BufferedMessage(nil), f.readBatch...), nil
}
func (f *fakeBuffer) Ack(_ context.Context, ids []string) error {
	f.ackIDs = append(f.ackIDs, ids...)
	return nil
}
func (f *fakeBuffer) ClaimStale(context.Context, time.Duration, int) ([]domain.BufferedMessage, error) {
	return nil, nil
}
func (f *fakeBuffer) IncrementRetry(context.Context, string) (int, error) { return 0, nil }
func (f *fakeBuffer) ResetRetry(_ context.Context, ids []string) error {
	f.resetIDs = append(f.resetIDs, ids...)
	return nil
}
func (f *fakeBuffer) Deadletter(context.Context, domain.BufferedMessage, string, int) error {
	return nil
}
func (f *fakeBuffer) Length(context.Context) (int64, error) { return 0, nil }

type fakeRepo struct {
	inserted int
	err      error
}

func (f *fakeRepo) InsertBatchIdempotent(context.Context, []domain.EnrichedTelemetry) (int, error) {
	return f.inserted, f.err
}
func (f *fakeRepo) GetLatest(context.Context, string) (*domain.EnrichedTelemetry, error) {
	return nil, nil
}
func (f *fakeRepo) QueryRange(context.Context, domain.TelemetryRangeQuery) ([]domain.EnrichedTelemetry, error) {
	return nil, nil
}
func (f *fakeRepo) Ping(context.Context) error { return nil }

type fakeProfiles struct{}

func (fakeProfiles) Match(domain.RawTelemetryPayload) (*domain.DeviceProfile, error) {
	return &domain.DeviceProfile{ID: "power_meter_9"}, nil
}
func (fakeProfiles) Transform(domain.RawTelemetryPayload, *domain.DeviceProfile) (map[string]any, error) {
	return map[string]any{"voltage_r": 22.75}, nil
}

type noopMetrics struct{}

func (noopMetrics) IncMQTTReceived()                   {}
func (noopMetrics) IncMQTTRejected(string)             {}
func (noopMetrics) IncRedisXAddError()                 {}
func (noopMetrics) IncWorkerProcessed()                {}
func (noopMetrics) IncWorkerDuplicate()                {}
func (noopMetrics) IncWorkerFailed(string)             {}
func (noopMetrics) IncDeadlettered()                   {}
func (noopMetrics) ObserveBatchInsertDuration(float64) {}

func TestProcessBatchAcksAfterSuccessfulInsert(t *testing.T) {
	t.Parallel()

	buffer := &fakeBuffer{readBatch: []domain.BufferedMessage{{ID: "1", Payload: domain.RawTelemetryPayload{DeviceID: "office-eng", Timestamp: "100", Values: []int{1}, MessageID: "msg-1", ProfileID: "power_meter_9"}}}}
	repo := &fakeRepo{inserted: 1}
	worker := NewWorkerService(buffer, repo, fakeProfiles{}, noopMetrics{}, logging.New("error"), 5)

	if err := worker.ProcessBatch(context.Background(), 10); err != nil {
		t.Fatalf("process batch failed: %v", err)
	}

	if len(buffer.ackIDs) != 1 || buffer.ackIDs[0] != "1" {
		t.Fatalf("expected ack after insert, got %v", buffer.ackIDs)
	}
	if len(buffer.resetIDs) != 1 || buffer.resetIDs[0] != "1" {
		t.Fatalf("expected retry reset after ack, got %v", buffer.resetIDs)
	}
}

func TestProcessBatchAcksAfterDuplicateInsert(t *testing.T) {
	t.Parallel()

	buffer := &fakeBuffer{readBatch: []domain.BufferedMessage{{ID: "2", Payload: domain.RawTelemetryPayload{DeviceID: "office-eng", Timestamp: "100", Values: []int{1}, MessageID: "msg-1", ProfileID: "power_meter_9"}}}}
	repo := &fakeRepo{inserted: 0}
	worker := NewWorkerService(buffer, repo, fakeProfiles{}, noopMetrics{}, logging.New("error"), 5)

	if err := worker.ProcessBatch(context.Background(), 10); err != nil {
		t.Fatalf("process batch failed: %v", err)
	}

	if len(buffer.ackIDs) != 1 || buffer.ackIDs[0] != "2" {
		t.Fatalf("expected ack for duplicate-safe insert, got %v", buffer.ackIDs)
	}
}
