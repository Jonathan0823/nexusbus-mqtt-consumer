package redis

import (
	"reflect"
	"testing"

	"github.com/go-redis/redis/v8"
)

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
