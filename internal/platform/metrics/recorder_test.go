package metrics

import (
	"strings"
	"sync"
	"testing"
)

func TestRecorder_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	r := NewRecorder()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reason := "reason-a"
			if i%2 == 0 {
				reason = "reason-b"
			}
			r.IncMQTTRejected(reason)
			r.IncWorkerFailed(reason)
			r.IncMQTTReceived()
			r.Snapshot()
		}(i)
	}
	wg.Wait()

	s := r.Snapshot()
	if s.MQTTReceivedTotal != 50 {
		t.Fatalf("expected 50 MQTT received, got %d", s.MQTTReceivedTotal)
	}
	if s.MQTTRejectedTotal["reason-a"] == 0 || s.MQTTRejectedTotal["reason-b"] == 0 {
		t.Fatalf("expected both reject reasons to be counted: %#v", s.MQTTRejectedTotal)
	}
	if s.WorkerFailedTotal["reason-a"] == 0 || s.WorkerFailedTotal["reason-b"] == 0 {
		t.Fatalf("expected both failed reasons to be counted: %#v", s.WorkerFailedTotal)
	}
}

func TestRecorderStringSumsRejectReasons(t *testing.T) {
	t.Parallel()

	r := NewRecorder()
	r.IncMQTTRejected("reason-a")
	r.IncMQTTRejected("reason-b")

	output := r.String()
	if !strings.Contains(output, "mqtt_messages_rejected_total 2") {
		t.Fatalf("expected summed rejected total in metrics output, got: %s", output)
	}
}
