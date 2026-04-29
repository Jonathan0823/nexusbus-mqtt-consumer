package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"modbus-mqtt-consumer/internal/core/domain"
	"modbus-mqtt-consumer/internal/platform/logging"
)

type fakeBufferCoalesceStream struct {
	readBatch     []domain.BufferedMessage
	ackedIDs      []string
	resetRetryIDs []string
	claimed       []domain.BufferedMessage
	deadlettered  []domain.BufferedMessage
	retryCount    int
	ackErr        error
}

func (f *fakeBufferCoalesceStream) Add(context.Context, domain.RawTelemetryMessage) error { return nil }
func (f *fakeBufferCoalesceStream) ReadBatch(context.Context, int) ([]domain.BufferedMessage, error) {
	return append([]domain.BufferedMessage(nil), f.readBatch...), nil
}
func (f *fakeBufferCoalesceStream) Ack(_ context.Context, ids []string) error {
	if f.ackErr != nil {
		return f.ackErr
	}
	f.ackedIDs = append(f.ackedIDs, ids...)
	return nil
}
func (f *fakeBufferCoalesceStream) ClaimStale(context.Context, time.Duration, int) ([]domain.BufferedMessage, error) {
	return append([]domain.BufferedMessage(nil), f.claimed...), nil
}
func (f *fakeBufferCoalesceStream) IncrementRetry(context.Context, string) (int, error) {
	return f.retryCount, nil
}

func (f *fakeBufferCoalesceStream) ResetRetry(_ context.Context, ids []string) error {
	f.resetRetryIDs = append(f.resetRetryIDs, ids...)
	return nil
}

func (f *fakeBufferCoalesceStream) Deadletter(_ context.Context, msg domain.BufferedMessage, _ string, _ int) error {
	f.deadlettered = append(f.deadlettered, msg)
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

type failingTransformProfilesCoalesceStream struct{}

func (failingTransformProfilesCoalesceStream) Match(payload domain.RawTelemetryPayload) (*domain.DeviceProfile, error) {
	return &domain.DeviceProfile{ID: "power_meter_9"}, nil
}

func (failingTransformProfilesCoalesceStream) Transform(payload domain.RawTelemetryPayload, profile *domain.DeviceProfile) (map[string]any, error) {
	return nil, errors.New("transform failed")
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

func TestCoalescingWorker_StartFlushLoopUsesFreshContext(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{}
	repo := &fakeRepoCoalesceStream{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 5, time.Second)

	msg := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	if err := w.BufferOne(msg); err != nil {
		t.Fatalf("BufferOne failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		w.StartFlushLoop(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StartFlushLoop did not exit")
	}

	if len(buf.ackedIDs) != 1 {
		t.Fatalf("expected 1 acked ID on shutdown flush, got %d: %v", len(buf.ackedIDs), buf.ackedIDs)
	}
}

func TestCoalescingWorker_DeadlettersAndAcksFailedEntries(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{retryCount: 3}
	repo := &fakeRepoCoalesceStream{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 3, time.Second)

	msg := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	if err := w.BufferOne(msg); err != nil {
		t.Fatalf("BufferOne failed: %v", err)
	}

	w.profiles = failingTransformProfilesCoalesceStream{}
	w.Flush(context.Background())

	if len(buf.deadlettered) != 1 {
		t.Fatalf("expected 1 deadlettered message, got %d", len(buf.deadlettered))
	}
	if len(buf.ackedIDs) != 1 {
		t.Fatalf("expected deadlettered stream ID to be acked, got %d: %v", len(buf.ackedIDs), buf.ackedIDs)
	}
	if len(buf.resetRetryIDs) != 1 {
		t.Fatalf("expected deadlettered stream ID retry counter to be reset, got %d: %v", len(buf.resetRetryIDs), buf.resetRetryIDs)
	}
}

func TestCoalescingWorker_DeadletterAckFailureDoesNotResetRetry(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{retryCount: 3, ackErr: errors.New("ack failed")}
	repo := &fakeRepoCoalesceStream{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 3, time.Second)

	msg := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	if err := w.BufferOne(msg); err != nil {
		t.Fatalf("BufferOne failed: %v", err)
	}

	w.profiles = failingTransformProfilesCoalesceStream{}
	w.Flush(context.Background())

	if len(buf.deadlettered) != 1 {
		t.Fatalf("expected 1 deadlettered message, got %d", len(buf.deadlettered))
	}
	if len(buf.ackedIDs) != 0 {
		t.Fatalf("expected no ack on ack failure, got %d", len(buf.ackedIDs))
	}
	if len(buf.resetRetryIDs) != 0 {
		t.Fatalf("expected no retry reset on ack failure, got %d", len(buf.resetRetryIDs))
	}
}

// Test that Flush works with a fresh context (not canceled).
func TestCoalescingWorker_FlushWithFreshContext(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{}
	repo := &fakeRepoCoalesceStream{inserted: 1}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 5, time.Second)

	msg := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	w.BufferOne(msg)

	// Use a fresh context, not the one that might be canceled
	ctx := context.Background()
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if len(buf.ackedIDs) != 1 {
		t.Fatalf("expected 1 acked ID, got %d", len(buf.ackedIDs))
	}
}

// Test that failed entries trigger retry counting.
func TestCoalescingWorker_FailedEntriesRetry(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{}
	repo := &fakeRepoCoalesceStream{err: errors.New("insert failed")}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 3, time.Second)

	msg := domain.BufferedMessage{
		ID:      "1",
		Payload: domain.RawTelemetryPayload{DeviceID: "device-1", Timestamp: "100"},
	}
	w.BufferOne(msg)

	// Flush will fail on insert, triggering retry handling
	w.Flush(context.Background())

	// The message should not be acked (will be retried)
	if len(buf.ackedIDs) != 0 {
		t.Fatalf("expected 0 acked IDs (should retry), got %d", len(buf.ackedIDs))
	}
}

// Test concurrent BufferOne calls are safe (no data race).
func TestCoalescingWorker_ConcurrentBufferOne(t *testing.T) {
	t.Parallel()

	buf := &fakeBufferCoalesceStream{}
	repo := &fakeRepoCoalesceStream{inserted: 100}
	w := NewCoalescingWorker(buf, repo, fakeProfilesCoalesceStream{}, fakeMetricsCoalesceStream{}, logging.New("error"), 5, time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				msg := domain.BufferedMessage{
					ID:      fmt.Sprintf("%d-%d", idx, j),
					Payload: domain.RawTelemetryPayload{DeviceID: fmt.Sprintf("device-%d", idx), Timestamp: "100"},
				}
				w.BufferOne(msg)
			}
		}(i)
	}
	wg.Wait()

	// Should have 10 devices buffered
	count := w.PendingCount()
	if count != 10 {
		t.Fatalf("expected 10 pending entries, got %d", count)
	}
}
