package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"

	"modbus-mqtt-consumer/internal/core/ports"
)

// Recorder implements MetricsRecorder using Prometheus-style in-memory metrics.
type Recorder struct {
	mu                     sync.RWMutex
	mqttReceivedTotal      atomic.Int64
	mqttRejectedTotal      map[string]*atomic.Int64
	redisXAddErrorTotal    atomic.Int64
	workerProcessedTotal   atomic.Int64
	workerDuplicateTotal   atomic.Int64
	workerFailedTotal      map[string]*atomic.Int64
	deadletteredTotal      atomic.Int64
	batchInsertDurationsMs atomic.Int64
	batchInsertCount       atomic.Int64
}

// NewRecorder creates a new metrics recorder.
func NewRecorder() *Recorder {
	rejected := make(map[string]*atomic.Int64)
	failed := make(map[string]*atomic.Int64)

	return &Recorder{
		mqttRejectedTotal: rejected,
		workerFailedTotal: failed,
	}
}

// incMap increments a counter in a map, creating if needed.
func (r *Recorder) incMap(m map[string]*atomic.Int64, key string) {
	_, ok := m[key]
	if !ok {
		m[key] = &atomic.Int64{}
	}
	m[key].Add(1)
}

// IncMQTTReceived increments the MQTT received counter.
func (r *Recorder) IncMQTTReceived() {
	r.mqttReceivedTotal.Add(1)
}

// IncMQTTRejected increments the MQTT rejected counter by reason.
func (r *Recorder) IncMQTTRejected(reason string) {
	r.incMap(r.mqttRejectedTotal, reason)
}

// IncRedisXAddError increments the Redis XADD error counter.
func (r *Recorder) IncRedisXAddError() {
	r.redisXAddErrorTotal.Add(1)
}

// IncWorkerProcessed increments the worker processed counter.
func (r *Recorder) IncWorkerProcessed() {
	r.workerProcessedTotal.Add(1)
}

// IncWorkerDuplicate increments the worker duplicate counter.
func (r *Recorder) IncWorkerDuplicate() {
	r.workerDuplicateTotal.Add(1)
}

// IncWorkerFailed increments the worker failed counter by reason.
func (r *Recorder) IncWorkerFailed(reason string) {
	r.incMap(r.workerFailedTotal, reason)
}

// IncDeadlettered increments the deadlettered counter.
func (r *Recorder) IncDeadlettered() {
	r.deadletteredTotal.Add(1)
}

// ObserveBatchInsertDuration records batch insert duration in seconds.
func (r *Recorder) ObserveBatchInsertDuration(seconds float64) {
	r.batchInsertDurationsMs.Add(int64(seconds * 1000))
	r.batchInsertCount.Add(1)
}

// Metrics represents the current metrics state.
type Metrics struct {
	MQTTReceivedTotal    int64
	MQTTRejectedTotal    map[string]int64
	RedisXAddErrorTotal  int64
	WorkerProcessedTotal int64
	WorkerDuplicateTotal int64
	WorkerFailedTotal    map[string]int64
	DeadletteredTotal    int64
	AvgBatchInsertMs     float64
}

// Snapshot returns a snapshot of current metrics.
func (r *Recorder) Snapshot() Metrics {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rejected := make(map[string]int64)
	for k, v := range r.mqttRejectedTotal {
		rejected[k] = v.Load()
	}

	failed := make(map[string]int64)
	for k, v := range r.workerFailedTotal {
		failed[k] = v.Load()
	}

	avgMs := 0.0
	if r.batchInsertCount.Load() > 0 {
		avgMs = float64(r.batchInsertDurationsMs.Load()) / float64(r.batchInsertCount.Load())
	}

	return Metrics{
		MQTTReceivedTotal:    r.mqttReceivedTotal.Load(),
		MQTTRejectedTotal:    rejected,
		RedisXAddErrorTotal:  r.redisXAddErrorTotal.Load(),
		WorkerProcessedTotal: r.workerProcessedTotal.Load(),
		WorkerDuplicateTotal: r.workerDuplicateTotal.Load(),
		WorkerFailedTotal:    failed,
		DeadletteredTotal:    r.deadletteredTotal.Load(),
		AvgBatchInsertMs:     avgMs,
	}
}

// String returns a Prometheus-formatted string.
func (r *Recorder) String() string {
	s := r.Snapshot()
	return fmt.Sprintf(`# HELP mqtt_messages_received_total Total MQTT messages received
# TYPE mqtt_messages_received_total counter
mqtt_messages_received_total %d
# HELP mqtt_messages_rejected_total Total MQTT messages rejected
# TYPE mqtt_messages_rejected_total counter
mqtt_messages_rejected_total %d
# HELP redis_xadd_error_total Total Redis XADD errors
# TYPE redis_xadd_error_total counter
redis_xadd_error_total %d
# HELP worker_messages_processed_total Total worker messages processed
# TYPE worker_messages_processed_total counter
worker_messages_processed_total %d
# HELP worker_messages_duplicate_total Total worker duplicate messages
# TYPE worker_messages_duplicate_total counter
worker_messages_duplicate_total %d
# HELP worker_messages_deadlettered_total Total messages deadlettered
# TYPE worker_messages_deadlettered_total counter
worker_messages_deadlettered_total %d
# HELP worker_batch_insert_duration_ms_avg Average batch insert duration in ms
# TYPE worker_batch_insert_duration_ms_avg gauge
worker_batch_insert_duration_ms_avg %.2f
`, s.MQTTReceivedTotal, s.MQTTRejectedTotal["total"], s.RedisXAddErrorTotal,
		s.WorkerProcessedTotal, s.WorkerDuplicateTotal, s.DeadletteredTotal, s.AvgBatchInsertMs)
}

// Compile-time check that Recorder implements ports.MetricsRecorder.
var _ ports.MetricsRecorder = (*Recorder)(nil)
