package service

import (
	"context"
	"testing"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

type fakeBufferCoalesce struct {
	ackedIDs []string
}

func (f *fakeBufferCoalesce) Add(context.Context, domain.RawTelemetryMessage) error { return nil }
func (f *fakeBufferCoalesce) ReadBatch(context.Context, int) ([]domain.BufferedMessage, error) {
	return nil, nil
}
func (f *fakeBufferCoalesce) Ack(_ context.Context, ids []string) error {
	f.ackedIDs = append(f.ackedIDs, ids...)
	return nil
}
func (f *fakeBufferCoalesce) ClaimStale(context.Context, time.Duration, int) ([]domain.BufferedMessage, error) {
	return nil, nil
}
func (f *fakeBufferCoalesce) IncrementRetry(context.Context, string) (int, error) { return 0, nil }
func (f *fakeBufferCoalesce) ResetRetry(_ context.Context, ids []string) error    { return nil }
func (f *fakeBufferCoalesce) Deadletter(context.Context, domain.BufferedMessage, string, int) error {
	return nil
}
func (f *fakeBufferCoalesce) Length(context.Context) (int64, error) { return 0, nil }

type fakeRepoCoalesce struct {
	inserted int
	err      error
}

func (f *fakeRepoCoalesce) InsertBatchIdempotent(context.Context, []domain.EnrichedTelemetry) (int, error) {
	return f.inserted, f.err
}
func (f *fakeRepoCoalesce) GetLatest(context.Context, string) (*domain.EnrichedTelemetry, error) {
	return nil, nil
}
func (f *fakeRepoCoalesce) QueryRange(context.Context, domain.TelemetryRangeQuery) ([]domain.EnrichedTelemetry, error) {
	return nil, nil
}
func (f *fakeRepoCoalesce) Ping(context.Context) error { return nil }

type fakeProfilesCoalesce struct{}

func (fakeProfilesCoalesce) Match(payload domain.RawTelemetryPayload) (*domain.DeviceProfile, error) {
	return &domain.DeviceProfile{ID: "power_meter_9"}, nil
}
func (fakeProfilesCoalesce) Transform(payload domain.RawTelemetryPayload, profile *domain.DeviceProfile) (map[string]any, error) {
	return map[string]any{"voltage": 220.0}, nil
}

type fakeMetricsCoalesce struct{}

func (fakeMetricsCoalesce) IncMQTTReceived()                   {}
func (fakeMetricsCoalesce) IncMQTTRejected(string)             {}
func (fakeMetricsCoalesce) IncRedisXAddError()                 {}
func (fakeMetricsCoalesce) IncWorkerProcessed()                {}
func (fakeMetricsCoalesce) IncWorkerDuplicate()                {}
func (fakeMetricsCoalesce) IncWorkerFailed(string)             {}
func (fakeMetricsCoalesce) IncDeadlettered()                   {}
func (fakeMetricsCoalesce) ObserveBatchInsertDuration(float64) {}

func TestCoalescingWorker_ReplacesOlderMessage(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesce{}
	repo := &fakeRepoCoalesce{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesce{}, fakeMetricsCoalesce{}, logging.New("error"), 5, time.Second)

	// First message at time 100.
	msg1 := domain.BufferedMessage{
		ID: "1",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "100",
			Values:    []int{100},
		},
	}

	if err := w.Handle(context.Background(), msg1); err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if w.PendingCount() != 1 {
		t.Fatalf("expected 1 pending, got %d", w.PendingCount())
	}

	// Second message at time 200 (newer).
	msg2 := domain.BufferedMessage{
		ID: "2",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "200",
			Values:    []int{200},
		},
	}

	if err := w.Handle(context.Background(), msg2); err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	// Should still be 1 (replaced).
	if w.PendingCount() != 1 {
		t.Fatalf("expected 1 pending after replace, got %d", w.PendingCount())
	}
}

func TestCoalescingWorker_KeepsNewerMessage(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesce{}
	repo := &fakeRepoCoalesce{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesce{}, fakeMetricsCoalesce{}, logging.New("error"), 5, time.Second)

	// Old message.
	msgOld := domain.BufferedMessage{
		ID: "1",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "100",
			Values:    []int{100},
		},
	}
	w.Handle(context.Background(), msgOld)

	// Newer message should replace.
	msgNew := domain.BufferedMessage{
		ID: "2",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "200",
			Values:    []int{200},
		},
	}
	w.Handle(context.Background(), msgNew)

	if w.PendingCount() != 1 {
		t.Fatalf("expected 1, got %d", w.PendingCount())
	}
}

func TestCoalescingWorker_IgnoresOlderMessage(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesce{}
	repo := &fakeRepoCoalesce{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesce{}, fakeMetricsCoalesce{}, logging.New("error"), 5, time.Second)

	// Newer first.
	msgNew := domain.BufferedMessage{
		ID: "2",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "200",
			Values:    []int{200},
		},
	}
	w.Handle(context.Background(), msgNew)

	// Older should be ignored.
	msgOld := domain.BufferedMessage{
		ID: "1",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "100",
			Values:    []int{100},
		},
	}
	w.Handle(context.Background(), msgOld)

	if w.PendingCount() != 1 {
		t.Fatalf("expected 1, got %d", w.PendingCount())
	}
}

func TestCoalescingWorker_DifferentDevices(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesce{}
	repo := &fakeRepoCoalesce{inserted: 2}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesce{}, fakeMetricsCoalesce{}, logging.New("error"), 5, time.Second)

	// Two different devices.
	msg1 := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	msg2 := domain.BufferedMessage{
		ID:      "2",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-2", Timestamp: "100"},
	}

	w.Handle(context.Background(), msg1)
	w.Handle(context.Background(), msg2)

	if w.PendingCount() != 2 {
		t.Fatalf("expected 2, got %d", w.PendingCount())
	}
}

func TestCoalescingWorker_PendingCount(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesce{}
	repo := &fakeRepoCoalesce{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesce{}, fakeMetricsCoalesce{}, logging.New("error"), 5, time.Second)

	count := w.PendingCount()
	if count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}

	msg := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	w.Handle(context.Background(), msg)

	if w.PendingCount() != 1 {
		t.Fatalf("expected 1, got %d", w.PendingCount())
	}
}
