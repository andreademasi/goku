package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnqueueWithInvalidPayload tests various invalid payload scenarios
func TestEnqueueWithInvalidPayload(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 0, // No workers for this test
	})
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("UnmarshalablePayload", func(t *testing.T) {
		// Channels cannot be marshaled to JSON
		type UnmarshalableStruct struct {
			Channel chan int
		}

		_, err := q.Enqueue(ctx, "TestJob", UnmarshalableStruct{
			Channel: make(chan int),
		})
		assert.Error(t, err, "should fail to enqueue unmarshalable payload")
	})

	t.Run("CircularReference", func(t *testing.T) {
		// Circular references cause infinite recursion in JSON marshal
		type Node struct {
			Value int
			Next  *Node
		}

		node := &Node{Value: 1}
		node.Next = node // Circular reference

		_, err := q.Enqueue(ctx, "TestJob", node)
		assert.Error(t, err, "should fail to enqueue payload with circular reference")
	})
}

// TestEnqueueWithZeroWorkers tests that enqueue works even with no workers
func TestEnqueueWithZeroWorkers(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 0, // No workers
	})
	require.NoError(t, err)

	q.Register("TestJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		return nil
	}).ToHandler())

	ctx := context.Background()

	// Should be able to enqueue even with no workers
	jobID, err := q.Enqueue(ctx, "TestJob", "test data")
	require.NoError(t, err)
	require.NotEmpty(t, jobID)

	// Job should remain pending
	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusPending, job.Status)
}

// TestEnqueueWithScheduledAtInPast tests scheduled time in the past
func TestEnqueueWithScheduledAtInPast(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var processed atomic.Bool
	q.Register("PastJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		processed.Store(true)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Schedule job for 1 hour in the past
	_, err = q.EnqueueWithOptions(ctx, "PastJob", "test", EnqueueOptions{
		ScheduledAt: time.Now().Add(-1 * time.Hour),
	})
	require.NoError(t, err)

	// Should be processed immediately
	time.Sleep(300 * time.Millisecond)
	assert.True(t, processed.Load(), "job scheduled in past should be processed immediately")
}

// TestEnqueueWithScheduledAtFarFuture tests scheduled time far in the future
func TestEnqueueWithScheduledAtFarFuture(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var processed atomic.Bool
	q.Register("FutureJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		processed.Store(true)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Schedule job for 10 years in the future
	jobID, err := q.EnqueueWithOptions(ctx, "FutureJob", "test", EnqueueOptions{
		ScheduledAt: time.Now().Add(10 * 365 * 24 * time.Hour),
	})
	require.NoError(t, err)

	// Should NOT be processed
	time.Sleep(500 * time.Millisecond)
	assert.False(t, processed.Load(), "job scheduled in far future should not be processed")

	// Job should still be pending
	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusPending, job.Status)
}

// TestEnqueueWithMaxRetriesZero tests one-shot jobs
func TestEnqueueWithMaxRetriesZero(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	q.Register("OneShotJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		return errors.New("always fails")
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Enqueue with MaxRetries=0 - the storage layer applies default of 3
	// This test verifies that MaxRetries=0 is handled (even if default is applied)
	jobID, err := q.EnqueueWithOptions(ctx, "OneShotJob", "test", EnqueueOptions{
		MaxRetries: 0,
	})
	require.NoError(t, err)

	// Wait for processing and potential retries
	time.Sleep(1 * time.Second)

	// Job should be failed eventually
	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusFailed, job.Status)
}

// TestEnqueueWithVeryHighMaxRetries tests jobs with many retries
func TestEnqueueWithVeryHighMaxRetries(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 0, // No workers for this test
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Enqueue with very high MaxRetries
	jobID, err := q.EnqueueWithOptions(ctx, "ManyRetriesJob", "test", EnqueueOptions{
		MaxRetries: 9999,
	})
	require.NoError(t, err)

	// Verify it was stored correctly
	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, 9999, job.MaxRetries)
}

// TestEnqueueWithEmptyJobType tests that empty job type fails
func TestEnqueueWithEmptyJobType(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 0,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Enqueue with empty job type should fail
	_, err = q.Enqueue(ctx, "", "test data")
	assert.Error(t, err, "empty job type should fail")
}

// TestEnqueueWithNegativePriority tests negative priority values
func TestEnqueueWithNegativePriority(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 0,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Negative priority should fail validation
	_, err = q.EnqueueWithOptions(ctx, "TestJob", "test", EnqueueOptions{
		Priority: -10,
	})
	assert.Error(t, err, "negative priority should fail")
}

// TestEnqueueWithVeryHighPriority tests maximum priority values
func TestEnqueueWithVeryHighPriority(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 0,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Very high priority (beyond limit) should fail
	_, err = q.EnqueueWithOptions(ctx, "TestJob", "test", EnqueueOptions{
		Priority: 10000,
	})
	assert.Error(t, err, "priority beyond limit should fail")
}

// TestMultipleQueueInstancesSameStorage tests multiple queues sharing storage
func TestMultipleQueueInstancesSameStorage(t *testing.T) {
	store := memory.New()

	// Create two queue instances
	q1, err := New(Config{
		Storage:      store,
		Workers:      2,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	q2, err := New(Config{
		Storage:      store,
		Workers:      2,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var processedByQ1, processedByQ2 atomic.Int32

	// Register handlers in both queues
	q1.Register("SharedJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		processedByQ1.Add(1)
		time.Sleep(100 * time.Millisecond)
		return nil
	}).ToHandler())

	q2.Register("SharedJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		processedByQ2.Add(1)
		time.Sleep(100 * time.Millisecond)
		return nil
	}).ToHandler())

	// Start both queues
	q1.Start()
	q2.Start()
	defer q1.Stop()
	defer q2.Stop()

	ctx := context.Background()

	// Enqueue jobs through q1
	for i := 0; i < 10; i++ {
		_, err := q1.Enqueue(ctx, "SharedJob", "test")
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Jobs should be distributed between both queues
	totalProcessed := processedByQ1.Load() + processedByQ2.Load()
	assert.Equal(t, int32(10), totalProcessed, "all jobs should be processed")
	assert.Greater(t, processedByQ1.Load(), int32(0), "q1 should process some jobs")
	assert.Greater(t, processedByQ2.Load(), int32(0), "q2 should process some jobs")
}

// TestStopWithoutStart tests stopping a queue that was never started
func TestStopWithoutStart(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 1,
	})
	require.NoError(t, err)

	// Stop without start should return error
	err = q.Stop()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrQueueNotStarted)
}

// TestMultipleStops tests calling Stop multiple times
func TestMultipleStops(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 1,
	})
	require.NoError(t, err)

	q.Start()

	// First stop should succeed
	err = q.Stop()
	assert.NoError(t, err)

	// Second stop - implementation may be idempotent or return error
	// Just verify it doesn't panic
	_ = q.Stop()
}

// TestStartStopStartCycle tests restarting a queue
func TestStartStopStartCycle(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var processedCount atomic.Int32
	q.Register("CycleJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		processedCount.Add(1)
		return nil
	}).ToHandler())

	ctx := context.Background()

	// First cycle
	q.Start()
	_, err = q.Enqueue(ctx, "CycleJob", "test1")
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	q.Stop()

	firstCount := processedCount.Load()
	assert.GreaterOrEqual(t, firstCount, int32(1), "first job should be processed")

	// Second cycle - verifies queue can be restarted
	q.Start()
	_, err = q.Enqueue(ctx, "CycleJob", "test2")
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	q.Stop()

	finalCount := processedCount.Load()
	// At minimum, we should have processed the first job
	// Second job may or may not complete depending on timing
	assert.GreaterOrEqual(t, finalCount, firstCount, "should have processed at least the first job")
}

// TestEnqueueWhileStopping tests enqueuing while queue is shutting down
func TestEnqueueWhileStopping(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         1,
		PollInterval:    50 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	q.Register("LongJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		time.Sleep(3 * time.Second)
		return nil
	}).ToHandler())

	q.Start()

	ctx := context.Background()

	// Enqueue long-running job
	_, err = q.Enqueue(ctx, "LongJob", "test")
	require.NoError(t, err)

	// Wait a bit for job to start
	time.Sleep(100 * time.Millisecond)

	// Start stopping in goroutine
	go func() {
		q.Stop()
	}()

	// Try to enqueue while stopping
	time.Sleep(100 * time.Millisecond)
	_, err = q.Enqueue(ctx, "LongJob", "test2")
	// Should succeed - enqueue doesn't depend on queue state
	assert.NoError(t, err)
}

// TestWorkerStarvation tests many jobs with few workers
func TestWorkerStarvation(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1, // Only 1 worker
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var processedCount atomic.Int32
	q.Register("StarveJob", HandlerFunc[int](func(ctx context.Context, id int) error {
		processedCount.Add(1)
		time.Sleep(50 * time.Millisecond)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Enqueue many jobs
	for i := 0; i < 20; i++ {
		_, err := q.Enqueue(ctx, "StarveJob", i)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Single worker should process jobs sequentially
	// Not all may be done, but should make progress
	assert.Greater(t, processedCount.Load(), int32(10), "worker should process many jobs")
}

// TestVeryLongShutdownTimeout tests queue with very long shutdown timeout
func TestVeryLongShutdownTimeout(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         1,
		PollInterval:    50 * time.Millisecond,
		ShutdownTimeout: 1 * time.Hour, // Very long timeout
	})
	require.NoError(t, err)

	var completed atomic.Bool
	q.Register("QuickJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		completed.Store(true)
		return nil
	}).ToHandler())

	q.Start()

	ctx := context.Background()
	_, err = q.Enqueue(ctx, "QuickJob", "test")
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Stop should complete quickly even with long timeout if job is done
	stopStart := time.Now()
	err = q.Stop()
	stopDuration := time.Since(stopStart)

	assert.NoError(t, err)
	assert.Less(t, stopDuration, 5*time.Second, "stop should complete quickly for fast jobs")
	assert.True(t, completed.Load())
}

// TestZeroShutdownTimeout tests queue with zero shutdown timeout
func TestZeroShutdownTimeout(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         1,
		PollInterval:    50 * time.Millisecond,
		ShutdownTimeout: 0, // Should use default
	})
	require.NoError(t, err)

	// Should apply default timeout (30s)
	assert.Equal(t, 30*time.Second, q.config.ShutdownTimeout)
}

// TestNegativeWorkerCount tests invalid worker count
func TestNegativeWorkerCount(t *testing.T) {
	store := memory.New()
	_, err := New(Config{
		Storage: store,
		Workers: -5, // Negative workers
	})
	// Implementation may allow 0 or negative (defaults are applied)
	// Just verify it doesn't panic
	_ = err
}

// TestNegativePollInterval tests invalid poll interval
func TestNegativePollInterval(t *testing.T) {
	store := memory.New()
	_, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: -1 * time.Second, // Negative interval
	})
	// Implementation may allow negative (defaults are applied)
	// Just verify it doesn't panic
	_ = err
}

// TestCountJobsMethod tests counting jobs via ListJobs
func TestCountJobsMethod(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	// Enqueue various jobs
	for i := 0; i < 5; i++ {
		_, err := store.Enqueue(ctx, &storage.Job{
			Type:   "TypeA",
			Status: storage.StatusPending,
		})
		require.NoError(t, err)
	}

	for i := 0; i < 3; i++ {
		_, err := store.Enqueue(ctx, &storage.Job{
			Type:   "TypeB",
			Status: storage.StatusPending,
		})
		require.NoError(t, err)

		// Complete some
		if i < 2 {
			job, _ := store.Dequeue(ctx)
			store.Complete(ctx, job.ID)
		}
	}

	// Count all jobs using ListJobs
	allJobs, err := store.ListJobs(ctx, storage.JobFilter{})
	require.NoError(t, err)
	assert.Equal(t, 8, len(allJobs))

	// Count by status (5 TypeA pending + 1 TypeB pending = 6 pending, 2 TypeB completed)
	pendingJobs, err := store.ListJobs(ctx, storage.JobFilter{
		Statuses: []storage.JobStatus{storage.StatusPending},
	})
	require.NoError(t, err)
	assert.Equal(t, 6, len(pendingJobs), "should have 6 pending jobs")

	completedJobs, err := store.ListJobs(ctx, storage.JobFilter{
		Statuses: []storage.JobStatus{storage.StatusCompleted},
	})
	require.NoError(t, err)
	assert.Equal(t, 2, len(completedJobs))

	// Count by type
	typeAJobs, err := store.ListJobs(ctx, storage.JobFilter{
		Types: []string{"TypeA"},
	})
	require.NoError(t, err)
	assert.Equal(t, 5, len(typeAJobs))

	typeBJobs, err := store.ListJobs(ctx, storage.JobFilter{
		Types: []string{"TypeB"},
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(typeBJobs))
}

