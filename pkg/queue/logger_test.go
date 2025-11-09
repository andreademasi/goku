package queue

import (
	"testing"

	"github.com/andreademasi/goku/pkg/queue/storage/memory"
)

func TestNoopLogger(t *testing.T) {
	logger := NewNoopLogger()

	logger.Debug("test")
	logger.Info("test")
	logger.Warn("test")
	logger.Error("test")
}

func TestLogLevel(t *testing.T) {
	t.Run("String representation", func(t *testing.T) {
		tests := []struct {
			level    LogLevel
			expected string
		}{
			{LogLevelDebug, "DEBUG"},
			{LogLevelInfo, "INFO"},
			{LogLevelWarn, "WARN"},
			{LogLevelError, "ERROR"},
			{LogLevelNone, "NONE"},
		}

		for _, tt := range tests {
			if tt.level.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.level.String())
			}
		}
	})

	t.Run("Parse log level", func(t *testing.T) {
		tests := []struct {
			input    string
			expected LogLevel
		}{
			{"DEBUG", LogLevelDebug},
			{"debug", LogLevelDebug},
			{"INFO", LogLevelInfo},
			{"info", LogLevelInfo},
			{"WARN", LogLevelWarn},
			{"warn", LogLevelWarn},
			{"WARNING", LogLevelWarn},
			{"ERROR", LogLevelError},
			{"error", LogLevelError},
			{"NONE", LogLevelNone},
			{"none", LogLevelNone},
			{"invalid", LogLevelInfo}, // Default to Info
		}

		for _, tt := range tests {
			result := ParseLogLevel(tt.input)
			if result != tt.expected {
				t.Errorf("ParseLogLevel(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		}
	})

	t.Run("Level ordering", func(t *testing.T) {
		if !(LogLevelDebug < LogLevelInfo) {
			t.Error("Debug should be less than Info")
		}
		if !(LogLevelInfo < LogLevelWarn) {
			t.Error("Info should be less than Warn")
		}
		if !(LogLevelWarn < LogLevelError) {
			t.Error("Warn should be less than Error")
		}
		if !(LogLevelError < LogLevelNone) {
			t.Error("Error should be less than None")
		}
	})
}

func TestLoggerInQueue(t *testing.T) {
	t.Run("Queue uses default noop logger", func(t *testing.T) {
		store := memory.New()
		q, err := New(Config{
			Storage: store,
			Workers: 1,
		})
		if err != nil {
			t.Fatalf("Failed to create queue: %v", err)
		}

		if err := q.Start(); err != nil {
			t.Fatalf("Failed to start queue: %v", err)
		}
		defer q.Stop()
	})

	t.Run("Queue accepts custom logger", func(t *testing.T) {
		store := memory.New()
		logger := NewNoopLogger()

		q, err := New(Config{
			Storage: store,
			Workers: 1,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create queue: %v", err)
		}

		if err := q.Start(); err != nil {
			t.Fatalf("Failed to start queue: %v", err)
		}
		defer q.Stop()
	})
}

func BenchmarkNoopLogger(b *testing.B) {
	logger := NewNoopLogger()

	for i := 0; i < b.N; i++ {
		logger.Info("test message", "key1", "value1", "key2", 42)
	}
}
