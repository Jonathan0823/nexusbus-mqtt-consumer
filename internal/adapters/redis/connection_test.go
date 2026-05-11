package redis

import "testing"

func TestRedactHost_FailsClosedOnParseError(t *testing.T) {
	t.Parallel()

	got := redactHost("://bad-host-value")
	if got != "<redacted>" {
		t.Fatalf("redactHost() = %q, want <redacted>", got)
	}
}

func TestRedactHost_RedactsPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		exclude string
		contain string
	}{
		{
			name:    "redis url with password",
			input:   "redis://:foobared@localhost:6379",
			exclude: "foobared",
			contain: "localhost:6379",
		},
		{
			name:    "postgres url dsn",
			input:   "postgres://postgres:secret@10.19.16.31:5432/iot_db?sslmode=disable",
			exclude: "secret",
			contain: "10.19.16.31:5432",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := redactHost(tc.input)
			if tc.exclude != "" && contains(got, tc.exclude) {
				t.Errorf("redactHost(%q) = %q; must NOT contain %q", tc.input, got, tc.exclude)
			}
			if tc.contain != "" && !contains(got, tc.contain) {
				t.Errorf("redactHost(%q) = %q; must contain %q", tc.input, got, tc.contain)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (s[:len(substr)] == substr || contains(s[1:], substr)))
}
