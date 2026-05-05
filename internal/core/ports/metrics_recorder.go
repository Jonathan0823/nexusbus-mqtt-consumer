package ports

// MetricsRecorder defines the interface for recording operational metrics.
type MetricsRecorder interface {
	IncMQTTReceived()
	IncMQTTRejected(reason string)
	IncRedisXAddError()
	IncWorkerProcessed()
	IncWorkerDuplicate()
	IncWorkerFailed(reason string)
	IncDeadlettered()
	ObserveBatchInsertDuration(seconds float64)
}