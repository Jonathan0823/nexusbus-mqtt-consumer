package domain

import "testing"

func TestRawTelemetryPayloadIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload RawTelemetryPayload
		wantErr bool
	}{
		{name: "valid", payload: RawTelemetryPayload{DeviceID: "dev-1", Values: []int{1, 2}, Count: 2}},
		{name: "empty values", payload: RawTelemetryPayload{DeviceID: "dev-1"}, wantErr: true},
		{name: "negative address", payload: RawTelemetryPayload{DeviceID: "dev-1", Values: []int{1}, Address: -1}, wantErr: true},
		{name: "negative count", payload: RawTelemetryPayload{DeviceID: "dev-1", Values: []int{1}, Count: -1}, wantErr: true},
		{name: "count mismatch", payload: RawTelemetryPayload{DeviceID: "dev-1", Values: []int{1, 2}, Count: 3}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.payload.IsValid()
			if tt.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}
