package memory_test

import (
	"context"
	"sync"
	"testing"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	store := memory.New()
	assert.NotNil(t, store)
	defer store.Close()
}

func TestMemorySpecific_IDGeneration(t *testing.T) {
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	job1 := &storage.Job{Type: "test", Payload: []byte(`{}`)}
	job2 := &storage.Job{Type: "test", Payload: []byte(`{}`)}

	id1, err := store.Enqueue(ctx, job1)
	require.NoError(t, err)

	id2, err := store.Enqueue(ctx, job2)
	require.NoError(t, err)

	// IDs should be unique and sequential
	assert.NotEqual(t, id1, id2)
	assert.Contains(t, id1, "mem-")
	assert.Contains(t, id2, "mem-")
}

func TestMemorySpecific_NoSideEffects(t *testing.T) {
	// Test that retrieving a job doesn't modify it
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	job := &storage.Job{Type: "test", Payload: []byte(`{"data":"value"}`)}
	id, _ := store.Enqueue(ctx, job)

	retrieved1, _ := store.GetJob(ctx, id)
	retrieved2, _ := store.GetJob(ctx, id)

	// Both retrievals should return the same data
	assert.JSONEq(t, string(retrieved1.Payload), string(retrieved2.Payload))
	assert.Equal(t, retrieved1.Type, retrieved2.Type)
	assert.Equal(t, retrieved1.Status, retrieved2.Status)
}

func TestMemorySpecific_ConcurrentEnqueue(t *testing.T) {
	// Test that concurrent enqueues don't cause data races or conflicts
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	const numGoroutines = 50
	const jobsPerGoroutine = 10

	var wg sync.WaitGroup
	var mu sync.Mutex
	allIDs := make(map[string]bool)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < jobsPerGoroutine; j++ {
				job := &storage.Job{
					Type:    "concurrent_test",
					Payload: []byte(`{}`),
				}

				id, err := store.Enqueue(ctx, job)
				require.NoError(t, err)

				mu.Lock()
				// Verify ID is unique
				assert.False(t, allIDs[id], "duplicate ID generated: %s", id)
				allIDs[id] = true
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Verify all IDs are unique
	assert.Equal(t, numGoroutines*jobsPerGoroutine, len(allIDs))
}

func TestMemorySpecific_IsolationBetweenInstances(t *testing.T) {
	// Test that different storage instances don't share data
	store1 := memory.New()
	defer store1.Close()

	store2 := memory.New()
	defer store2.Close()

	ctx := context.Background()

	// Enqueue job in store1 only
	job := &storage.Job{Type: "test", Payload: []byte(`{"store":"1"}`)}
	id1, err := store1.Enqueue(ctx, job)
	require.NoError(t, err)

	// Enqueue different job in store2 only
	job2 := &storage.Job{Type: "test", Payload: []byte(`{"store":"2"}`)}
	id2, err := store2.Enqueue(ctx, job2)
	require.NoError(t, err)

	// Get job from store1
	retrieved1, err := store1.GetJob(ctx, id1)
	require.NoError(t, err)
	assert.JSONEq(t, `{"store":"1"}`, string(retrieved1.Payload))

	// Get job from store2
	retrieved2, err := store2.GetJob(ctx, id2)
	require.NoError(t, err)
	assert.JSONEq(t, `{"store":"2"}`, string(retrieved2.Payload))

	// Verify stores don't interfere with each other
	// Each store should only have 1 job
	jobs1, err := store1.ListJobs(ctx, storage.JobFilter{})
	require.NoError(t, err)
	assert.Len(t, jobs1, 1)

	jobs2, err := store2.ListJobs(ctx, storage.JobFilter{})
	require.NoError(t, err)
	assert.Len(t, jobs2, 1)
}

func TestMemorySpecific_DequeueExhaustion(t *testing.T) {
	// Test that dequeuing from empty queue repeatedly works correctly
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	// Dequeue from empty queue multiple times
	for i := 0; i < 10; i++ {
		job, err := store.Dequeue(ctx)
		require.NoError(t, err)
		assert.Nil(t, job, "attempt %d should return nil", i)
	}
}

func TestMemorySpecific_CompleteNonexistentJob(t *testing.T) {
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	err := store.Complete(ctx, "nonexistent-id")
	assert.ErrorIs(t, err, storage.ErrJobNotFound)
}

func TestMemorySpecific_FailNonexistentJob(t *testing.T) {
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	err := store.Fail(ctx, "nonexistent-id", "some error")
	assert.ErrorIs(t, err, storage.ErrJobNotFound)
}

func TestMemorySpecific_RetryNonexistentJob(t *testing.T) {
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	err := store.Retry(ctx, "nonexistent-id")
	assert.ErrorIs(t, err, storage.ErrJobNotFound)
}

func TestMemorySpecific_PingAlwaysSucceeds(t *testing.T) {
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	// Ping should always succeed for in-memory
	err := store.Ping(ctx)
	assert.NoError(t, err)

	// Even after close, ping is a no-op
	store.Close()
	err = store.Ping(ctx)
	assert.NoError(t, err)
}

func TestMemorySpecific_ListJobsEmptyFilter(t *testing.T) {
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	// Enqueue some jobs
	for i := 0; i < 5; i++ {
		job := &storage.Job{Type: "test", Priority: i}
		_, err := store.Enqueue(ctx, job)
		require.NoError(t, err)
	}

	// List with empty filter should return all jobs
	jobs, err := store.ListJobs(ctx, storage.JobFilter{})
	require.NoError(t, err)
	assert.Len(t, jobs, 5)
}

func TestMemorySpecific_ContextCancellation(t *testing.T) {
	store := memory.New()
	defer store.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Operations should respect context cancellation
	_, err := store.Enqueue(ctx, &storage.Job{Type: "test"})
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)

	_, err = store.Dequeue(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMemorySpecific_NilJob(t *testing.T) {
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	_, err := store.Enqueue(ctx, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job is nil")
}

func TestMemorySpecific_EmptyJobType(t *testing.T) {
	store := memory.New()
	defer store.Close()
	ctx := context.Background()

	job := &storage.Job{Payload: []byte(`{}`)}
	_, err := store.Enqueue(ctx, job)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job type is required")
}

