package runtime

import (
	"context"
	"testing"
	"time"

	"modbus-mqtt-consumer/internal/platform/config"
	"modbus-mqtt-consumer/internal/platform/logging"
)

func TestAppRunWaitsForShutdownCompletion(t *testing.T) {
	t.Parallel()

	app := NewApp(&config.Config{}, logging.New("error"))
	done := make(chan struct{})
	app.AddComponentFunc("slow-close", func() error {
		time.Sleep(50 * time.Millisecond)
		close(done)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	if err := app.Run(ctx); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected shutdown component to complete before run returned")
	}
}
