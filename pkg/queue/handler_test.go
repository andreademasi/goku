package queue

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandlerPanic tests that panics in handlers are recovered and jobs are marked as failed
func TestHandlerPanic(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      2,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// Register handler that panics
	q.Register("PanicJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		panic("unexpected panic in handler")
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "PanicJob", "test data")
	require.NoError(t, err)

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Job should be marked as failed
	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusFailed, job.Status, "panicked job should be marked as failed")
	assert.NotNil(t, job.Error)
	assert.Contains(t, *job.Error, "panic", "error should mention panic")
}

// TestHandlerPanicDoesNotCrashWorker tests that a panic doesn't crash the worker
func TestHandlerPanicDoesNotCrashWorker(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1, // Single worker to verify it continues working
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var processedAfterPanic atomic.Int32

	// Register handler that panics
	q.Register("PanicJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		panic("worker crash test")
	}).ToHandler())

	// Register normal handler
	q.Register("NormalJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		processedAfterPanic.Add(1)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Enqueue panic job
	_, err = q.Enqueue(ctx, "PanicJob", "panic")
	require.NoError(t, err)

	// Wait for panic to be handled
	time.Sleep(300 * time.Millisecond)

	// Enqueue normal job after panic
	_, err = q.Enqueue(ctx, "NormalJob", "normal")
	require.NoError(t, err)

	// Wait for normal job to be processed
	time.Sleep(500 * time.Millisecond)

	// Worker should have recovered and processed the normal job
	assert.Equal(t, int32(1), processedAfterPanic.Load(),
		"worker should continue processing jobs after panic")
}

// TestMultiplePanicsInDifferentWorkers tests concurrent panics
func TestMultiplePanicsInDifferentWorkers(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      5,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	q.Register("PanicJob", HandlerFunc[int](func(ctx context.Context, id int) error {
		panic(fmt.Sprintf("panic from job %d", id))
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Enqueue multiple panic jobs
	jobIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		jobIDs[i], err = q.Enqueue(ctx, "PanicJob", i)
		require.NoError(t, err)
	}

	// Wait for all to be processed
	time.Sleep(2 * time.Second)

	// All jobs should be marked as failed
	failedCount := 0
	for _, jobID := range jobIDs {
		job, err := store.GetJob(ctx, jobID)
		require.NoError(t, err)
		if job.Status == storage.StatusFailed {
			failedCount++
		}
	}

	assert.Equal(t, 10, failedCount, "all panicked jobs should be marked as failed")
}

// TestHandlerReturnsNil tests that nil errors are handled correctly
func TestHandlerReturnsNil(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	q.Register("SuccessJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "SuccessJob", "test")
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusCompleted, job.Status)
	assert.Nil(t, job.Error)
}

// TestHandlerReturnsError tests that errors are properly recorded
func TestHandlerReturnsError(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	testErr := errors.New("custom error message")

	q.Register("ErrorJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		return testErr
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "ErrorJob", "test")
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusFailed, job.Status)
	assert.NotNil(t, job.Error)
	assert.Contains(t, *job.Error, "custom error message")
}

// TestHandlerContextCancellation tests that handlers respect context cancellation
func TestHandlerContextCancellation(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         1,
		PollInterval:    50 * time.Millisecond,
		ShutdownTimeout: 100 * time.Millisecond, // Short timeout
	})
	require.NoError(t, err)

	var handlerStarted, handlerFinished atomic.Bool

	q.Register("LongJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		handlerStarted.Store(true)
		defer handlerFinished.Store(true)

		// Simulate long-running work
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}).ToHandler())

	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "LongJob", "test")
	require.NoError(t, err)

	q.Start()

	// Wait for handler to start
	time.Sleep(200 * time.Millisecond)
	assert.True(t, handlerStarted.Load(), "handler should have started")

	// Stop queue (should cancel context)
	stopStart := time.Now()
	err = q.Stop()
	stopDuration := time.Since(stopStart)

	// Stop should respect ShutdownTimeout
	assert.Less(t, stopDuration, 2*time.Second, "stop should not wait too long")

	// Handler should have finished (either completed or cancelled)
	assert.True(t, handlerFinished.Load(), "handler should have finished")

	// Job should be failed due to context cancellation
	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	// Job is either failed or still running depending on timing
	assert.Contains(t, []storage.JobStatus{storage.StatusFailed, storage.StatusRunning},
		job.Status, "job should be failed or running")
}

// TestHandlerWithVeryLargePayload tests handling of large payloads
func TestHandlerWithVeryLargePayload(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var receivedDataSize atomic.Int32

	q.Register("LargePayloadJob", HandlerFunc[map[string]string](func(ctx context.Context, data map[string]string) error {
		receivedDataSize.Store(int32(len(data["large_field"])))
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Create large payload (1MB string)
	largeString := string(make([]byte, 1024*1024))
	jobID, err := q.Enqueue(ctx, "LargePayloadJob", map[string]string{
		"large_field": largeString,
	})
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusCompleted, job.Status)
	assert.Equal(t, int32(1024*1024), receivedDataSize.Load(), "handler should receive full payload")
}

// TestHandlerWithNilPayload tests handling of nil payloads
func TestHandlerWithNilPayload(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var receivedNil atomic.Bool

	q.Register("NilPayloadJob", HandlerFunc[*string](func(ctx context.Context, data *string) error {
		if data == nil {
			receivedNil.Store(true)
		}
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	
	// Enqueue with nil payload
	var nilString *string
	_, err = q.Enqueue(ctx, "NilPayloadJob", nilString)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, receivedNil.Load(), "handler should receive nil payload")
}

// TestHandlerSlowProcessing tests that slow handlers don't block other workers
func TestHandlerSlowProcessing(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      3,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var slowCompleted, fastCompleted atomic.Int32

	q.Register("SlowJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		time.Sleep(2 * time.Second)
		slowCompleted.Add(1)
		return nil
	}).ToHandler())

	q.Register("FastJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		time.Sleep(100 * time.Millisecond)
		fastCompleted.Add(1)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Enqueue 1 slow job
	_, err = q.Enqueue(ctx, "SlowJob", "slow")
	require.NoError(t, err)

	// Enqueue 5 fast jobs
	for i := 0; i < 5; i++ {
		_, err = q.Enqueue(ctx, "FastJob", "fast")
		require.NoError(t, err)
	}

	// Wait for fast jobs to complete
	time.Sleep(1 * time.Second)

	// All fast jobs should be done, slow job still processing
	assert.Equal(t, int32(5), fastCompleted.Load(), "fast jobs should complete")
	assert.Equal(t, int32(0), slowCompleted.Load(), "slow job should still be processing")

	// Wait for slow job
	time.Sleep(2 * time.Second)
	assert.Equal(t, int32(1), slowCompleted.Load(), "slow job should eventually complete")
}

// TestHandlerErrorWrapping tests that errors are properly wrapped with context
func TestHandlerErrorWrapping(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	innerErr := errors.New("inner error")

	q.Register("WrappedErrorJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		return fmt.Errorf("wrapped: %w", innerErr)
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "WrappedErrorJob", "test")
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusFailed, job.Status)
	assert.NotNil(t, job.Error)
	assert.Contains(t, *job.Error, "wrapped")
	assert.Contains(t, *job.Error, "inner error")
}

// CustomError is a test error type with code and message
type CustomError struct {
	Code    int
	Message string
}

func (e CustomError) Error() string {
	return fmt.Sprintf("error %d: %s", e.Code, e.Message)
}

// TestHandlerReturnsDifferentErrorTypes tests various error types
func TestHandlerReturnsDifferentErrorTypes(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	q.Register("CustomErrorJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		return CustomError{Code: 500, Message: "internal error"}
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "CustomErrorJob", "test")
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusFailed, job.Status)
	assert.NotNil(t, job.Error)
	assert.Contains(t, *job.Error, "error 500")
	assert.Contains(t, *job.Error, "internal error")
}

// TestPanicWithString tests panic with string value
func TestPanicWithString(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	q.Register("StringPanicJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		panic("string panic message")
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "StringPanicJob", "test")
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusFailed, job.Status)
	assert.NotNil(t, job.Error)
	assert.Contains(t, *job.Error, "panic")
	assert.Contains(t, *job.Error, "string panic message")
}

// TestPanicWithError tests panic with error value
func TestPanicWithError(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	q.Register("ErrorPanicJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		panic(errors.New("error panic"))
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "ErrorPanicJob", "test")
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusFailed, job.Status)
	assert.NotNil(t, job.Error)
	assert.Contains(t, *job.Error, "panic")
}

// TestPanicWithNil tests panic with nil value (unusual but possible)
func TestPanicWithNil(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	q.Register("NilPanicJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		panic(nil)
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "NilPanicJob", "test")
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusFailed, job.Status)
	assert.NotNil(t, job.Error)
	assert.Contains(t, *job.Error, "panic")
}

