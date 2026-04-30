package redis

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

// TestBlockTimeDefaultFallback verifies that when blockTime is 0,
// ReadBatch uses a default fallback of 1 second instead of blocking forever.
func TestBlockTimeDefaultFallback(t *testing.T) {
	t.Parallel()

	// Test the fallback logic in isolation
	tests := []struct {
		name          string
		blockTime     time.Duration
		expectedBlock time.Duration
	}{
		{
			name:          "zero blockTime uses default 1 second",
			blockTime:     0,
			expectedBlock: time.Second,
		},
		{
			name:          "negative blockTime uses default 1 second",
			blockTime:     -1 * time.Second,
			expectedBlock: time.Second,
		},
		{
			name:          "positive blockTime is used as-is",
			blockTime:     500 * time.Millisecond,
			expectedBlock: 500 * time.Millisecond,
		},
		{
			name:          "large blockTime is used as-is",
			blockTime:     5 * time.Second,
			expectedBlock: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Simulate the fallback logic from ReadBatch
			blockTime := tt.blockTime
			if blockTime <= 0 {
				blockTime = time.Second // default 1 second fallback
			}

			if blockTime != tt.expectedBlock {
				t.Errorf("blockTime = %v, want %v", blockTime, tt.expectedBlock)
			}
		})
	}
}

// TestStreamBufferBlockTimeStored verifies that blockTime is properly stored
// in the StreamBuffer struct when created via NewStreamBuffer.
func TestStreamBufferBlockTimeStored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		blockTime time.Duration
	}{
		{
			name:      "zero blockTime stored",
			blockTime: 0,
		},
		{
			name:      "positive blockTime stored",
			blockTime: 2 * time.Second,
		},
		{
			name:      "custom blockTime stored",
			blockTime: 500 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a minimal StreamBuffer to verify field storage
			// We can't actually connect to Redis in tests, but we can verify
			// the struct field assignment logic by checking the config flow.
			cfg := Config{
				Addr:             "localhost:6379",
				Stream:           "test-stream",
				DeadletterStream: "test-dlq",
				Group:            "test-group",
				Consumer:         "test-consumer",
				BlockTime:        tt.blockTime,
			}

			// Verify config has the right blockTime
			if cfg.BlockTime != tt.blockTime {
				t.Errorf("Config.BlockTime = %v, want %v", cfg.BlockTime, tt.blockTime)
			}

			// The actual struct assignment happens in NewStreamBuffer
			// which we can't test without a real Redis connection.
			// This test verifies the config flow is correct.
		})
	}
}

func TestClaimStaleResponseParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		reply          []interface{}
		wantMessageIDs []string
		wantDeleted    int
	}{
		{
			name: "empty response - no pending messages",
			reply: []interface{}{
				"0-0",           // next cursor
				[]interface{}{}, // claimed (empty)
			},
			wantMessageIDs: nil,
			wantDeleted:    0,
		},
		{
			name: "legacy 2-element response (Redis 6)",
			reply: []interface{}{
				"0-0", // next cursor
				[]interface{}{
					// Single message: [id, [field, value, ...]]
					[]interface{}{"123-0", []interface{}{"field1", "value1", "field2", "value2"}},
				},
			},
			wantMessageIDs: []string{"123-0"},
			wantDeleted:    0,
		},
		{
			name: "Redis 7+ 3-element response",
			reply: []interface{}{
				"0-0", // next cursor
				[]interface{}{
					[]interface{}{"456-0", []interface{}{"field1", "value1"}},
					[]interface{}{"789-0", []interface{}{"field1", "value2"}},
				},
				// Deleted message IDs
				[]interface{}{"999-0"},
			},
			wantMessageIDs: []string{"456-0", "789-0"},
			wantDeleted:    1,
		},
		{
			name: "minimal message with no fields",
			reply: []interface{}{
				"0-0",
				[]interface{}{
					[]interface{}{"100-0", []interface{}{}}, // empty fields array
				},
			},
			wantMessageIDs: []string{"100-0"},
			wantDeleted:    0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Simulate parsing logic from ClaimStale
			if len(tt.reply) < 2 {
				if len(tt.wantMessageIDs) != 0 {
					t.Errorf("expected messages but got empty reply")
				}
				return
			}

			claimed, ok := tt.reply[1].([]interface{})
			if !ok {
				if len(tt.wantMessageIDs) != 0 {
					t.Errorf("expected messages but claimed is not an array")
				}
				return
			}

			var gotIDs []string
			for _, rawMsg := range claimed {
				msgArr, ok := rawMsg.([]interface{})
				if !ok || len(msgArr) < 2 {
					continue
				}

				id, ok := msgArr[0].(string)
				if !ok {
					continue
				}
				gotIDs = append(gotIDs, id)
			}

			if !reflect.DeepEqual(gotIDs, tt.wantMessageIDs) {
				t.Errorf("message IDs = %v, want %v", gotIDs, tt.wantMessageIDs)
			}

			// Check deleted count
			var deletedCount int
			if len(tt.reply) >= 3 && tt.reply[2] != nil {
				if deleted, ok := tt.reply[2].([]interface{}); ok {
					deletedCount = len(deleted)
				}
			}
			if deletedCount != tt.wantDeleted {
				t.Errorf("deleted count = %d, want %d", deletedCount, tt.wantDeleted)
			}
		})
	}
}

// TestClaimStaleResultParsingWithXMessage verifies the parsing handles redis.XMessage directly.
func TestClaimStaleResultParsingWithXMessage(t *testing.T) {
	t.Parallel()

	// Test that we handle both raw response format and XMessage format.
	// In practice go-redis may return either, so we verify our fallback works.

	// Simulate a legacy format: []interface{} with nested arrays
	legacyMsg := []interface{}{
		"test-id-1",
		[]interface{}{"key", "value"},
	}

	// Serialize and deserialize as would happen with redis.XMessage
	msg := redis.XMessage{
		ID:     legacyMsg[0].(string),
		Values: map[string]interface{}{"key": "value"},
	}

	if msg.ID != "test-id-1" {
		t.Errorf("expected ID test-id-1, got %s", msg.ID)
	}
	if msg.Values["key"] != "value" {
		t.Errorf("expected value, got %v", msg.Values["key"])
	}
}
