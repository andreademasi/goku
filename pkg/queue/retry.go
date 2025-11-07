package queue

import (
	"math"
	"math/rand"
	"time"
)

// ExponentialBackoff implements exponential backoff: 1s, 2s, 4s, 8s, 16s...
type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (e ExponentialBackoff) NextRetry(retryCount int) time.Duration {
	base := e.BaseDelay
	if base <= 0 {
		base = 1 * time.Second
	}

	maxDelay := e.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 1 * time.Hour
	}

	multiplier := math.Pow(2, float64(retryCount))
	delay := min(time.Duration(float64(base)*multiplier), maxDelay)

	return delay
}

// ExponentialBackoffWithJitter adds randomness to prevent thundering herd.
type ExponentialBackoffWithJitter struct {
	BaseDelay    time.Duration
	MaxDelay     time.Duration
	JitterFactor float64
}

func (e ExponentialBackoffWithJitter) NextRetry(retryCount int) time.Duration {
	base := e.BaseDelay
	if base <= 0 {
		base = 1 * time.Second
	}

	maxDelay := e.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 1 * time.Hour
	}

	jitterFactor := e.JitterFactor
	if jitterFactor <= 0 {
		jitterFactor = 0.5
	}
	if jitterFactor > 1.0 {
		jitterFactor = 1.0
	}

	multiplier := math.Pow(2, float64(retryCount))
	delay := min(time.Duration(float64(base)*multiplier), maxDelay)

	jitterRange := 2 * jitterFactor
	jitter := (1.0 - jitterFactor) + (rand.Float64() * jitterRange)
	delay = time.Duration(float64(delay) * jitter)

	return delay
}

// LinearBackoff implements linear backoff: delay, 2*delay, 3*delay...
type LinearBackoff struct {
	Delay    time.Duration
	MaxDelay time.Duration
}

func (l LinearBackoff) NextRetry(retryCount int) time.Duration {
	delay := l.Delay
	if delay <= 0 {
		delay = 5 * time.Second
	}

	maxDelay := l.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Minute
	}

	calculated := delay * time.Duration(retryCount+1)

	if calculated > maxDelay {
		return maxDelay
	}

	return calculated
}

// ConstantBackoff uses the same delay for all retries.
type ConstantBackoff struct {
	Delay time.Duration
}

func (c ConstantBackoff) NextRetry(retryCount int) time.Duration {
	delay := c.Delay
	if delay <= 0 {
		delay = 10 * time.Second
	}
	return delay
}
