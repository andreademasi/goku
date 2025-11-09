package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExponentialBackoff tests exponential backoff strategy
func TestExponentialBackoff(t *testing.T) {
	strategy := ExponentialBackoff{
		BaseDelay: 1 * time.Second,
		MaxDelay:  1 * time.Minute,
	}

	tests := []struct {
		retryCount int
		wantMin    time.Duration
		wantMax    time.Duration
	}{
		{0, 1 * time.Second, 1 * time.Second},    // 2^0 = 1
		{1, 2 * time.Second, 2 * time.Second},    // 2^1 = 2
		{2, 4 * time.Second, 4 * time.Second},    // 2^2 = 4
		{3, 8 * time.Second, 8 * time.Second},    // 2^3 = 8
		{4, 16 * time.Second, 16 * time.Second},  // 2^4 = 16
		{5, 32 * time.Second, 32 * time.Second},  // 2^5 = 32
		{6, 60 * time.Second, 60 * time.Second},  // 2^6 = 64, capped at 60
		{10, 60 * time.Second, 60 * time.Second}, // Should cap at MaxDelay
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.retryCount)), func(t *testing.T) {
			delay := strategy.NextRetry(tt.retryCount)
			assert.GreaterOrEqual(t, delay, tt.wantMin, "delay should be at least %v", tt.wantMin)
			assert.LessOrEqual(t, delay, tt.wantMax, "delay should be at most %v", tt.wantMax)
		})
	}
}

// TestExponentialBackoffWithJitter tests exponential backoff with jitter
func TestExponentialBackoffWithJitter(t *testing.T) {
	strategy := ExponentialBackoffWithJitter{
		BaseDelay: 1 * time.Second,
		MaxDelay:  1 * time.Minute,
	}

	// Test that jitter produces values within expected range
	tests := []struct {
		retryCount int
		wantMin    time.Duration
		wantMax    time.Duration
	}{
		{0, 500 * time.Millisecond, 1500 * time.Millisecond}, // 1s ± 50%
		{1, 1 * time.Second, 3 * time.Second},                // 2s ± 50%
		{2, 2 * time.Second, 6 * time.Second},                // 4s ± 50%
		{3, 4 * time.Second, 12 * time.Second},               // 8s ± 50%
		{10, 0, 120 * time.Second},                           // With jitter can exceed MaxDelay
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.retryCount)), func(t *testing.T) {
			// Run multiple times to ensure jitter varies
			delays := make([]time.Duration, 10)
			for i := 0; i < 10; i++ {
				delays[i] = strategy.NextRetry(tt.retryCount)
				assert.GreaterOrEqual(t, delays[i], tt.wantMin, "delay should be at least %v", tt.wantMin)
				assert.LessOrEqual(t, delays[i], tt.wantMax, "delay should be at most %v", tt.wantMax)
			}

			// Verify jitter produces different values (not all the same)
			allSame := true
			for i := 1; i < len(delays); i++ {
				if delays[i] != delays[0] {
					allSame = false
					break
				}
			}
			assert.False(t, allSame, "jitter should produce varying delays")
		})
	}
}

// TestLinearBackoff tests linear backoff strategy
func TestLinearBackoff(t *testing.T) {
	strategy := LinearBackoff{
		Delay:    5 * time.Second,
		MaxDelay: 1 * time.Minute,
	}

	tests := []struct {
		retryCount int
		want       time.Duration
	}{
		{0, 5 * time.Second},
		{1, 10 * time.Second},
		{2, 15 * time.Second},
		{3, 20 * time.Second},
		{10, 55 * time.Second},
		{11, 60 * time.Second}, // Capped at MaxDelay
		{20, 60 * time.Second}, // Should stay capped
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.retryCount)), func(t *testing.T) {
			delay := strategy.NextRetry(tt.retryCount)
			assert.Equal(t, tt.want, delay)
		})
	}
}

// TestConstantBackoff tests constant backoff strategy
func TestConstantBackoff(t *testing.T) {
	strategy := ConstantBackoff{
		Delay: 10 * time.Second,
	}

	// Should always return the same delay
	for i := 0; i < 10; i++ {
		delay := strategy.NextRetry(i)
		assert.Equal(t, 10*time.Second, delay, "constant backoff should always return same delay")
	}
}

// TestExponentialBackoffDefaults tests default values
func TestExponentialBackoffDefaults(t *testing.T) {
	t.Run("ZeroBaseDelay", func(t *testing.T) {
		strategy := ExponentialBackoff{
			BaseDelay: 0,
			MaxDelay:  1 * time.Minute,
		}

		delay := strategy.NextRetry(0)
		assert.Equal(t, 1*time.Second, delay, "should use 1s as default base delay")
	})

	t.Run("ZeroMaxDelay", func(t *testing.T) {
		strategy := ExponentialBackoff{
			BaseDelay: 1 * time.Second,
			MaxDelay:  0,
		}

		delay := strategy.NextRetry(20)
		assert.LessOrEqual(t, delay, 1*time.Hour, "should cap at default max delay")
		assert.Greater(t, delay, 30*time.Minute, "should be significant with large retry count")
	})

	t.Run("BothZero", func(t *testing.T) {
		strategy := ExponentialBackoff{}

		delay := strategy.NextRetry(0)
		assert.Equal(t, 1*time.Second, delay)

		delay = strategy.NextRetry(20)
		assert.Equal(t, 1*time.Hour, delay)
	})
}

// TestLinearBackoffDefaults tests default values
func TestLinearBackoffDefaults(t *testing.T) {
	t.Run("ZeroDelay", func(t *testing.T) {
		strategy := LinearBackoff{
			Delay:    0,
			MaxDelay: 1 * time.Minute,
		}

		delay := strategy.NextRetry(0)
		assert.Equal(t, 5*time.Second, delay, "should use 5s as default delay")
	})

	t.Run("ZeroMaxDelay", func(t *testing.T) {
		strategy := LinearBackoff{
			Delay:    5 * time.Second,
			MaxDelay: 0,
		}

		delay := strategy.NextRetry(100)
		assert.LessOrEqual(t, delay, 5*time.Minute, "should cap at default max delay")
		assert.Equal(t, 5*time.Minute, delay, "large retry count should hit max")
	})
}

// TestConstantBackoffDefault tests default value
func TestConstantBackoffDefault(t *testing.T) {
	strategy := ConstantBackoff{
		Delay: 0,
	}

	delay := strategy.NextRetry(0)
	assert.Equal(t, 10*time.Second, delay, "should use 10s as default delay")
}

// TestNegativeRetryCount tests behavior with negative retry counts
func TestNegativeRetryCount(t *testing.T) {
	strategies := []struct {
		name     string
		strategy RetryStrategy
	}{
		{"ExponentialBackoff", ExponentialBackoff{}},
		{"ExponentialBackoffWithJitter", ExponentialBackoffWithJitter{}},
		{"LinearBackoff", LinearBackoff{}},
		{"ConstantBackoff", ConstantBackoff{}},
	}

	for _, s := range strategies {
		t.Run(s.name, func(t *testing.T) {
			// Should not panic with negative retry count - that's the main thing
			delay := s.strategy.NextRetry(-1)
			// Just verify it doesn't panic and returns something (even if it's 0)
			_ = delay
		})
	}
}

// TestVeryLargeRetryCount tests behavior with very large retry counts
func TestVeryLargeRetryCount(t *testing.T) {
	strategies := []struct {
		name     string
		strategy RetryStrategy
	}{
		{"ExponentialBackoff", ExponentialBackoff{MaxDelay: 1 * time.Hour}},
		{"ExponentialBackoffWithJitter", ExponentialBackoffWithJitter{MaxDelay: 1 * time.Hour}},
		{"LinearBackoff", LinearBackoff{MaxDelay: 1 * time.Hour}},
		{"ConstantBackoff", ConstantBackoff{Delay: 10 * time.Second}},
	}

	for _, s := range strategies {
		t.Run(s.name, func(t *testing.T) {
			// Should cap at reasonable values even with huge retry count
			delay := s.strategy.NextRetry(1000)
			assert.LessOrEqual(t, delay, 2*time.Hour, "should not produce unreasonably large delays")
		})
	}
}

// FibonacciBackoff is a custom retry strategy using Fibonacci sequence
type FibonacciBackoff struct{}

func (f FibonacciBackoff) NextRetry(retryCount int) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}

	// Calculate Fibonacci number
	a, b := 1, 1
	for i := 0; i < retryCount; i++ {
		a, b = b, a+b
	}

	delay := time.Duration(a) * time.Second
	if delay > 1*time.Hour {
		delay = 1 * time.Hour
	}
	return delay
}

// TestCustomRetryStrategy tests that custom strategies work
func TestCustomRetryStrategy(t *testing.T) {
	strategy := FibonacciBackoff{}

	tests := []struct {
		retryCount int
		want       time.Duration
	}{
		{0, 1 * time.Second},
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 3 * time.Second},
		{4, 5 * time.Second},
		{5, 8 * time.Second},
		{6, 13 * time.Second},
	}

	for _, tt := range tests {
		delay := strategy.NextRetry(tt.retryCount)
		assert.Equal(t, tt.want, delay)
	}
}

// TestRetryStrategyInQueue tests that retry strategies work in actual queue
func TestRetryStrategyInQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	store := memory.New()

	// Use a fast constant backoff for testing
	q, err := New(Config{
		Storage:       store,
		Workers:       1,
		PollInterval:  50 * time.Millisecond,
		RetryStrategy: ConstantBackoff{Delay: 100 * time.Millisecond},
	})
	require.NoError(t, err)

	// Track retry timings
	var mu sync.Mutex
	var attempts []time.Time

	q.Register("RetryJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, time.Now())
		if len(attempts) < 3 {
			return errors.New("fail") // Fail first 2 attempts
		}
		return nil // Succeed on 3rd attempt
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	_, err = q.EnqueueWithOptions(ctx, "RetryJob", "test", EnqueueOptions{
		MaxRetries: 5,
	})
	require.NoError(t, err)

	// Wait for retries
	time.Sleep(5 * time.Second)

	mu.Lock()
	attemptCount := len(attempts)
	mu.Unlock()

	// Should have 3 attempts (initial + 2 retries)
	assert.GreaterOrEqual(t, attemptCount, 3, "should have at least 3 attempts")
}

// TestExponentialBackoffOverflow tests that exponential backoff handles overflow gracefully
func TestExponentialBackoffOverflow(t *testing.T) {
	strategy := ExponentialBackoff{
		BaseDelay: 1 * time.Second,
		MaxDelay:  1 * time.Hour,
	}

	// Very large retry count could cause integer overflow
	// Should cap at MaxDelay instead of panicking or producing negative values
	delay := strategy.NextRetry(100)
	assert.Greater(t, delay, time.Duration(0), "delay should be positive")
	assert.LessOrEqual(t, delay, 1*time.Hour, "delay should be capped at MaxDelay")
}
