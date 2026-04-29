package service

import (
	"context"
	"testing"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

type fakeBufferCoalesceStream struct {
	readBatch []domain.BufferedMessage
	ackedIDs  []string
	claimed   []domain.BufferedMessage
}

func (f *fakeBufferCoalesceStream) Add(context.Context, domain.RawTelemetryMessage) error { return nil }
func (f *fakeBufferCoalesceStream) ReadBatch(context.Context, int) ([]domain.BufferedMessage, error) {
	return append([]domain.BufferedMessage(nil), f.readBatch...), nil
}
func (f *fakeBufferCoalesceStream) Ack(_ context.Context, ids []string) error {
	f.ackedIDs = append(f.ackedIDs, ids...)
	return nil
}
func (f *fakeBufferCoalesceStream) ClaimStale(context.Context, time.Duration, int) ([]domain.BufferedMessage, error) {
	return append([]domain.BufferedMessage(nil), f.claimed...), nil
}
func (f *fakeBufferCoalesceStream) IncrementRetry(context.Context, string) (int, error) {
	return 0, nil
}
func (f *fakeBufferCoalesceStream) ResetRetry(context.Context, []string) error { return nil }
func (f *fakeBufferCoalesceStream) Deadletter(context.Context, domain.BufferedMessage, string, int) error {
	return nil
}
func (f *fakeBufferCoalesceStream) Length(context.Context) (int64, error) { return 0, nil }

type fakeRepoCoalesceStream struct {
	inserted int
	err      error
}

func (f *fakeRepoCoalesceStream) InsertBatchIdempotent(context.Context, []domain.EnrichedTelemetry) (int, error) {
	return f.inserted, f.err
}
func (f *fakeRepoCoalesceStream) GetLatest(context.Context, string) (*domain.EnrichedTelemetry, error) {
	return nil, nil
}
func (f *fakeRepoCoalesceStream) QueryRange(context.Context, domain.TelemetryRangeQuery) ([]domain.EnrichedTelemetry, error) {
	return nil, nil
}
func (f *fakeRepoCoalesceStream) Ping(context.Context) error { return nil }

type fakeProfilesCoalesceStream struct{}

func (fakeProfilesCoalesceStream) Match(payload domain.RawTelemetryPayload) (*domain.DeviceProfile, error) {
	return &domain.DeviceProfile{ID: "power_meter_9"}, nil
}
func (fakeProfilesCoalesceStream) Transform(payload domain.RawTelemetryPayload, profile *domain.DeviceProfile) (map[string]any, error) {
	return map[string]any{"voltage": 220.0}, nil
}

type fakeMetricsCoalesceStream struct{}

func (fakeMetricsCoalesceStream) IncMQTTReceived()                   {}
func (fakeMetricsCoalesceStream) IncMQTTRejected(string)             {}
func (fakeMetricsCoalesceStream) IncRedisXAddError()                 {}
func (fakeMetricsCoalesceStream) IncWorkerProcessed()                {}
func (fakeMetricsCoalesceStream) IncWorkerDuplicate()                {}
func (fakeMetricsCoalesceStream) IncWorkerFailed(string)             {}
func (fakeMetricsCoalesceStream) IncDeadlettered()                   {}
func (fakeMetricsCoalesceStream) ObserveBatchInsertDuration(float64) {}

func TestCoalescingWorker_BuffersNewerMessage(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{}
	repo := &fakeRepoCoalesceStream{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 5, time.Second)

	msg1 := domain.BufferedMessage{
		ID: "1",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "100",
			Values:    []int{100},
		},
	}

	if err := w.BufferOne(msg1); err != nil {
		t.Fatalf("bufferOne failed: %v", err)
	}

	if w.PendingCount() != 1 {
		t.Fatalf("expected 1 pending, got %d", w.PendingCount())
	}

	msg2 := domain.BufferedMessage{
		ID: "2",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "200",
			Values:    []int{200},
		},
	}

	if err := w.BufferOne(msg2); err != nil {
		t.Fatalf("bufferOne failed: %v", err)
	}

	if w.PendingCount() != 1 {
		t.Fatalf("expected 1 pending after replace, got %d", w.PendingCount())
	}
}

func TestCoalescingWorker_IgnoresOlderMessage(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{}
	repo := &fakeRepoCoalesceStream{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 5, time.Second)

	msgNew := domain.BufferedMessage{
		ID: "2",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "200",
			Values:    []int{200},
		},
	}
	w.BufferOne(msgNew)

	msgOld := domain.BufferedMessage{
		ID: "1",
		Payload: domain.RawTelemetryPayload{
			DeviceID:  "device-1",
			Timestamp: "100",
			Values:    []int{100},
		},
	}
	w.BufferOne(msgOld)

	if w.PendingCount() != 1 {
		t.Fatalf("expected 1, got %d", w.PendingCount())
	}
}

func TestCoalescingWorker_DifferentDevices(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{}
	repo := &fakeRepoCoalesceStream{inserted: 2}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 5, time.Second)

	msg1 := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	msg2 := domain.BufferedMessage{
		ID:      "2",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-2", Timestamp: "100"},
	}

	w.BufferOne(msg1)
	w.BufferOne(msg2)

	if w.PendingCount() != 2 {
		t.Fatalf("expected 2, got %d", w.PendingCount())
	}
}

func TestCoalescingWorker_PendingCount(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{}
	repo := &fakeRepoCoalesceStream{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 5, time.Second)

	count := w.PendingCount()
	if count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}

	msg := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	w.BufferOne(msg)

	if w.PendingCount() != 1 {
		t.Fatalf("expected 1, got %d", w.PendingCount())
	}
}

func TestCoalescingWorker_AcksAllStreamIDs(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{}
	repo := &fakeRepoCoalesceStream{inserted: 2}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 5, time.Second)

	msg1 := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	msg2 := domain.BufferedMessage{
		ID:      "2",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "200"},
	}

	w.BufferOne(msg1)
	w.BufferOne(msg2)

	w.Flush(context.Background())

	if len(buf.ackedIDs) != 2 {
		t.Fatalf("expected 2 acked IDs, got %d: %v", len(buf.ackedIDs), buf.ackedIDs)
	}
}
