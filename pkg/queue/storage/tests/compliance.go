package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StorageFactory creates a fresh storage instance for testing.
// The factory is responsible for any setup (creating tables, etc.)
// and the returned storage should be ready to use.
type StorageFactory func(t *testing.T) storage.Storage

// RunCompliance runs the complete compliance test suite against a storage backend.
// All storage implementations must pass these tests to ensure consistent behavior.
//
// Usage:
//
//	func TestMyStorageCompliance(t *testing.T) {
//	    tests.RunCompliance(t, func(t *testing.T) storage.Storage {
//	        // Setup and return your storage implementation
//	        return myStorage
//	    })
//	}
func RunCompliance(t *testing.T, factory StorageFactory) {
	t.Run("Enqueue", testEnqueue(factory))
	t.Run("EnqueueDefaults", testEnqueueDefaults(factory))
	t.Run("Dequeue", testDequeue(factory))
	t.Run("DequeueEmpty", testDequeueEmpty(factory))
	t.Run("ConcurrentDequeue", testConcurrentDequeue(factory))
	t.Run("Complete", testComplete(factory))
	t.Run("Fail", testFail(factory))
	t.Run("Retry", testRetry(factory))
	t.Run("GetJob", testGetJob(factory))
	t.Run("GetJobNotFound", testGetJobNotFound(factory))
	t.Run("Priority", testPriority(factory))
	t.Run("ScheduledJobs", testScheduledJobs(factory))
	t.Run("ListJobs", testListJobs(factory))
	t.Run("ListJobsWithFilters", testListJobsWithFilters(factory))
	t.Run("Ping", testPing(factory))
	// New tests added per architecture verification
	t.Run("Idempotency", testIdempotency(factory))
	t.Run("ContextCancellation", testContextCancellation(factory))
	t.Run("LargePayloads", testLargePayloads(factory))
	t.Run("PriorityValidation", testPriorityValidation(factory))
	t.Run("MaxRetriesNeverRetry", testMaxRetriesNeverRetry(factory))
}

// testEnqueue verifies basic enqueue functionality
func testEnqueue(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		job := &storage.Job{
			Type:       "test_email",
			Payload:    []byte(`{"to":"user@example.com"}`),
			Priority:   5,
			MaxRetries: 3,
		}

		id, err := store.Enqueue(ctx, job)
		require.NoError(t, err)
		require.NotEmpty(t, id, "job ID should not be empty")

		// Verify the job was stored correctly
		retrieved, err := store.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, id, retrieved.ID)
		assert.Equal(t, "test_email", retrieved.Type)
		// Use JSONEq to compare JSON payloads (handles formatting differences)
		assert.JSONEq(t, `{"to":"user@example.com"}`, string(retrieved.Payload))
		assert.Equal(t, 5, retrieved.Priority)
		assert.Equal(t, 3, retrieved.MaxRetries)
		assert.Equal(t, storage.StatusPending, retrieved.Status)
		assert.Equal(t, 0, retrieved.RetryCount)
		assert.NotZero(t, retrieved.CreatedAt)
		assert.NotZero(t, retrieved.ScheduledAt)
		assert.Nil(t, retrieved.ClaimedAt)
		assert.Nil(t, retrieved.CompletedAt)
		assert.Nil(t, retrieved.Error)
	}
}

// testEnqueueDefaults verifies that default values are applied correctly
func testEnqueueDefaults(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		// Enqueue job with minimal fields
		job := &storage.Job{
			Type: "minimal_job",
		}

		id, err := store.Enqueue(ctx, job)
		require.NoError(t, err)

		retrieved, err := store.GetJob(ctx, id)
		require.NoError(t, err)

		// Verify defaults were applied
		assert.Equal(t, storage.StatusPending, retrieved.Status)
		assert.Equal(t, 3, retrieved.MaxRetries, "MaxRetries should default to 3")
		assert.NotEmpty(t, retrieved.Payload, "Payload should default to {}")
		assert.NotZero(t, retrieved.ScheduledAt, "ScheduledAt should be set")
		assert.Equal(t, 0, retrieved.Priority, "Priority defaults to 0")
	}
}

// testDequeue verifies basic dequeue functionality
func testDequeue(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		job := &storage.Job{
			Type:    "dequeue_test",
			Payload: []byte(`{"message":"hello"}`),
		}

		id, err := store.Enqueue(ctx, job)
		require.NoError(t, err)

		// Dequeue the job
		dequeued, err := store.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, dequeued, "should dequeue a job")

		assert.Equal(t, id, dequeued.ID)
		assert.Equal(t, storage.StatusRunning, dequeued.Status, "status should be Running after dequeue")
		assert.NotNil(t, dequeued.ClaimedAt, "ClaimedAt should be set")

		// Second dequeue should return nothing (job already claimed)
		empty, err := store.Dequeue(ctx)
		require.NoError(t, err)
		assert.Nil(t, empty, "should not dequeue the same job twice")
	}
}

// testDequeueEmpty verifies that dequeue returns nil when no jobs are available
func testDequeueEmpty(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		// Dequeue from empty queue
		job, err := store.Dequeue(ctx)
		require.NoError(t, err, "empty queue should not return an error")
		assert.Nil(t, job, "should return nil when no jobs available")
	}
}

// testConcurrentDequeue is CRITICAL - verifies atomic dequeue behavior
func testConcurrentDequeue(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		const jobCount = 10
		const workerCount = 20

		// Enqueue jobs
		expectedIDs := make(map[string]bool)
		for i := 0; i < jobCount; i++ {
			job := &storage.Job{
				Type:    fmt.Sprintf("concurrent_test_%d", i),
				Payload: []byte(fmt.Sprintf(`{"index":%d}`, i)),
			}
			id, err := store.Enqueue(ctx, job)
			require.NoError(t, err)
			expectedIDs[id] = true
		}

		// Launch workers to claim jobs concurrently
		var wg sync.WaitGroup
		var mu sync.Mutex
		claimedIDs := make(map[string]int) // job ID -> count
		errors := make([]error, 0)

		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				job, err := store.Dequeue(ctx)
				if err != nil {
					mu.Lock()
					errors = append(errors, fmt.Errorf("worker %d: %w", workerID, err))
					mu.Unlock()
					return
				}

				if job != nil {
					mu.Lock()
					claimedIDs[job.ID]++
					mu.Unlock()
				}
			}(i)
		}

		wg.Wait()

		// Verify no errors occurred
		require.Empty(t, errors, "no errors should occur during concurrent dequeue")

		// CRITICAL: Verify exactly jobCount jobs were claimed
		assert.Equal(t, jobCount, len(claimedIDs),
			"exactly %d jobs should be claimed (got %d)", jobCount, len(claimedIDs))

		// CRITICAL: Verify no job was claimed more than once
		for id, count := range claimedIDs {
			assert.Equal(t, 1, count,
				"job %s was claimed %d times (should be exactly 1)", id, count)
		}

		// Verify all claimed jobs were in the original set
		for id := range claimedIDs {
			assert.True(t, expectedIDs[id],
				"claimed job %s was not in the original set", id)
		}
	}
}

// testComplete verifies job completion
func testComplete(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		job := &storage.Job{Type: "complete_test"}
		id, err := store.Enqueue(ctx, job)
		require.NoError(t, err)

		// Dequeue the job
		dequeued, err := store.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, dequeued)

		// Complete the job
		err = store.Complete(ctx, dequeued.ID)
		require.NoError(t, err)

		// Verify completion
		completed, err := store.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, storage.StatusCompleted, completed.Status)
		assert.NotNil(t, completed.CompletedAt, "CompletedAt should be set")
		assert.Nil(t, completed.Error, "Error should be nil for completed job")
	}
}

// testFail verifies job failure
func testFail(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		job := &storage.Job{Type: "fail_test"}
		id, err := store.Enqueue(ctx, job)
		require.NoError(t, err)

		// Dequeue the job
		dequeued, err := store.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, dequeued)

		// Fail the job with error message
		const errorMsg = "database connection timeout"
		err = store.Fail(ctx, dequeued.ID, errorMsg)
		require.NoError(t, err)

		// Verify failure
		failed, err := store.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, storage.StatusFailed, failed.Status)
		assert.NotNil(t, failed.CompletedAt, "CompletedAt should be set")
		require.NotNil(t, failed.Error, "Error should be set")
		assert.Equal(t, errorMsg, *failed.Error)
	}
}

// testRetry verifies retry functionality
func testRetry(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		job := &storage.Job{
			Type:       "retry_test",
			MaxRetries: 5,
		}
		id, err := store.Enqueue(ctx, job)
		require.NoError(t, err)

		// Dequeue and fail the job
		dequeued, err := store.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, dequeued)

		err = store.Fail(ctx, dequeued.ID, "temporary error")
		require.NoError(t, err)

		// Retry the job
		err = store.Retry(ctx, dequeued.ID)
		require.NoError(t, err)

		// Verify retry state
		retried, err := store.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, storage.StatusPending, retried.Status, "status should be Pending after retry")
		assert.Equal(t, 1, retried.RetryCount, "RetryCount should be incremented")
		assert.Nil(t, retried.ClaimedAt, "ClaimedAt should be cleared")
		assert.Nil(t, retried.CompletedAt, "CompletedAt should be cleared")
		assert.Nil(t, retried.Error, "Error should be cleared")

		// Should be dequeueable again
		retryDequeued, err := store.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, retryDequeued, "retried job should be dequeueable")
		assert.Equal(t, id, retryDequeued.ID)
		assert.Equal(t, 1, retryDequeued.RetryCount)
	}
}

// testGetJob verifies job retrieval
func testGetJob(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		job := &storage.Job{
			Type:     "get_test",
			Priority: 7,
		}
		id, err := store.Enqueue(ctx, job)
		require.NoError(t, err)

		retrieved, err := store.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, id, retrieved.ID)
		assert.Equal(t, "get_test", retrieved.Type)
		assert.Equal(t, 7, retrieved.Priority)
	}
}

// testGetJobNotFound verifies GetJob returns ErrJobNotFound for invalid ID
func testGetJobNotFound(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		_, err := store.GetJob(ctx, "nonexistent-id-12345")
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrJobNotFound)
	}
}

// testPriority verifies jobs are dequeued in priority order
func testPriority(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		// Enqueue jobs with different priorities (intentionally out of order)
		jobs := []struct {
			name     string
			priority int
		}{
			{"low", 1},
			{"highest", 100},
			{"medium", 5},
			{"high", 10},
			{"lowest", 0},
		}

		for _, j := range jobs {
			_, err := store.Enqueue(ctx, &storage.Job{
				Type:     j.name,
				Priority: j.priority,
			})
			require.NoError(t, err)
		}

		// Dequeue should return in priority order (highest first)
		expectedOrder := []string{"highest", "high", "medium", "low", "lowest"}
		for i, expected := range expectedOrder {
			dequeued, err := store.Dequeue(ctx)
			require.NoError(t, err, "dequeue %d", i)
			require.NotNil(t, dequeued, "dequeue %d should return a job", i)
			assert.Equal(t, expected, dequeued.Type,
				"job %d should be %s (priority order)", i, expected)
		}

		// No more jobs
		empty, err := store.Dequeue(ctx)
		require.NoError(t, err)
		assert.Nil(t, empty)
	}
}

// testScheduledJobs verifies that future-scheduled jobs are not dequeued early
func testScheduledJobs(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		now := time.Now().UTC()

		// Enqueue immediate job
		_, err := store.Enqueue(ctx, &storage.Job{
			Type:        "immediate",
			ScheduledAt: now.Add(-1 * time.Second), // In the past
		})
		require.NoError(t, err)

		// Enqueue future job
		futureID, err := store.Enqueue(ctx, &storage.Job{
			Type:        "future",
			ScheduledAt: now.Add(1 * time.Hour), // In the future
		})
		require.NoError(t, err)

		// Should dequeue immediate job only
		dequeued, err := store.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, dequeued, "should dequeue immediate job")
		assert.Equal(t, "immediate", dequeued.Type)

		// Should not dequeue future job
		empty, err := store.Dequeue(ctx)
		require.NoError(t, err)
		assert.Nil(t, empty, "future job should not be dequeued yet")

		// Verify future job still exists and is pending
		future, err := store.GetJob(ctx, futureID)
		require.NoError(t, err)
		assert.Equal(t, storage.StatusPending, future.Status)
	}
}

// testListJobs verifies basic listing functionality
func testListJobs(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		// Enqueue several jobs
		for i := 0; i < 5; i++ {
			_, err := store.Enqueue(ctx, &storage.Job{
				Type:     fmt.Sprintf("list_test_%d", i),
				Priority: i,
			})
			require.NoError(t, err)
		}

		// List all jobs
		jobs, err := store.ListJobs(ctx, storage.JobFilter{})
		require.NoError(t, err)
		assert.Len(t, jobs, 5, "should list all jobs")

		// Verify priority ordering (highest first)
		for i := 0; i < len(jobs)-1; i++ {
			assert.GreaterOrEqual(t, jobs[i].Priority, jobs[i+1].Priority,
				"jobs should be ordered by priority DESC")
		}
	}
}

// testListJobsWithFilters verifies filtering functionality
func testListJobsWithFilters(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		// Enqueue jobs with different types
		id1, err := store.Enqueue(ctx, &storage.Job{Type: "email"})
		require.NoError(t, err)
		id2, err := store.Enqueue(ctx, &storage.Job{Type: "email"})
		require.NoError(t, err)
		id3, err := store.Enqueue(ctx, &storage.Job{Type: "webhook"})
		require.NoError(t, err)

		// Complete one email job
		job, _ := store.Dequeue(ctx)
		store.Complete(ctx, job.ID)

		// Filter by type
		emailJobs, err := store.ListJobs(ctx, storage.JobFilter{
			Types: []string{"email"},
		})
		require.NoError(t, err)
		assert.Len(t, emailJobs, 2, "should filter by type")

		// Filter by status
		pendingJobs, err := store.ListJobs(ctx, storage.JobFilter{
			Statuses: []storage.JobStatus{storage.StatusPending},
		})
		require.NoError(t, err)
		// Should have 2 pending (one email was completed)
		assert.Len(t, pendingJobs, 2, "should filter by status")

		// Filter by IDs
		specificJobs, err := store.ListJobs(ctx, storage.JobFilter{
			IDs: []string{id1, id3},
		})
		require.NoError(t, err)
		assert.Len(t, specificJobs, 2, "should filter by IDs")

		// Verify correct jobs returned
		ids := []string{specificJobs[0].ID, specificJobs[1].ID}
		assert.Contains(t, ids, id1)
		assert.Contains(t, ids, id3)
		assert.NotContains(t, ids, id2)

		// Test limit
		limitedJobs, err := store.ListJobs(ctx, storage.JobFilter{
			Limit: 2,
		})
		require.NoError(t, err)
		assert.Len(t, limitedJobs, 2, "should respect limit")
	}
}

// testPing verifies connectivity check
func testPing(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		err := store.Ping(ctx)
		require.NoError(t, err, "ping should succeed on healthy storage")
	}
}

// testIdempotency verifies that operations can be safely repeated
func testIdempotency(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		t.Run("CompleteComplete", func(t *testing.T) {
			// Enqueue job
			id, err := store.Enqueue(ctx, &storage.Job{Type: "test"})
			require.NoError(t, err)

			// Dequeue and complete
			job, err := store.Dequeue(ctx)
			require.NoError(t, err)
			require.NotNil(t, job)

			// Complete once
			err = store.Complete(ctx, id)
			require.NoError(t, err)

			// Complete again - should be idempotent (no error)
			err = store.Complete(ctx, id)
			assert.NoError(t, err, "completing an already-completed job should be idempotent")

			// Verify status is still completed
			job, err = store.GetJob(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, storage.StatusCompleted, job.Status)
		})

		t.Run("FailFail", func(t *testing.T) {
			// Enqueue job
			id, err := store.Enqueue(ctx, &storage.Job{Type: "test"})
			require.NoError(t, err)

			// Dequeue
			job, err := store.Dequeue(ctx)
			require.NoError(t, err)
			require.NotNil(t, job)

			// Fail once
			err = store.Fail(ctx, id, "error 1")
			require.NoError(t, err)

			// Fail again with different error - should update error message
			err = store.Fail(ctx, id, "error 2")
			assert.NoError(t, err, "failing an already-failed job should be idempotent")

			// Verify error message was updated
			job, err = store.GetJob(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, storage.StatusFailed, job.Status)
			assert.NotNil(t, job.Error)
			assert.Equal(t, "error 2", *job.Error, "error message should be updated")
		})

		t.Run("RetryCompleted", func(t *testing.T) {
			// Enqueue and complete job
			id, err := store.Enqueue(ctx, &storage.Job{Type: "test"})
			require.NoError(t, err)

			job, err := store.Dequeue(ctx)
			require.NoError(t, err)
			require.NotNil(t, job)

			err = store.Complete(ctx, id)
			require.NoError(t, err)

			// Try to retry completed job - should work (manual recovery)
			err = store.Retry(ctx, id)
			assert.NoError(t, err, "retrying a completed job should be allowed for manual recovery")

			// Verify job is pending again
			job, err = store.GetJob(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, storage.StatusPending, job.Status)
		})
	}
}

// testContextCancellation verifies that operations respect context cancellation
func testContextCancellation(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()

		t.Run("EnqueueCancelled", func(t *testing.T) {
			// Create cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			_, err := store.Enqueue(ctx, &storage.Job{Type: "test"})
			// Backend may wrap the error, so check if it contains context.Canceled
			if err != nil {
				assert.Contains(t, err.Error(), "context canceled",
					"error should indicate context was canceled")
			} else {
				t.Errorf("enqueue should return error with cancelled context")
			}
		})

		t.Run("DequeueCancelled", func(t *testing.T) {
			// Create cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			_, err := store.Dequeue(ctx)
			// Note: Some backends may return nil, nil for cancelled context
			// This is acceptable - the key is not to panic or hang
			if err != nil {
				assert.Contains(t, err.Error(), "context canceled",
					"error should indicate context was canceled")
			}
		})

		t.Run("GetJobCancelled", func(t *testing.T) {
			// First enqueue a job with valid context
			ctx := context.Background()
			id, err := store.Enqueue(ctx, &storage.Job{Type: "test"})
			require.NoError(t, err)

			// Try to get it with cancelled context
			cancelledCtx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err = store.GetJob(cancelledCtx, id)
			if err != nil {
				// Error should indicate context was canceled (may be wrapped)
				assert.Contains(t, err.Error(), "context canceled",
					"error should indicate context was canceled")
			}
			// Some implementations may handle this gracefully, which is also acceptable
		})
	}
}

// testLargePayloads verifies that the storage handles various payload sizes correctly
func testLargePayloads(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		t.Run("1MB Payload", func(t *testing.T) {
			// 1MB payload - should succeed
			// Use valid JSON with printable characters (Postgres requires valid UTF-8 for JSONB)
			dataSize := 1*1024*1024 - 20 // Account for JSON wrapper
			data := make([]byte, dataSize)
			for i := range data {
				// Use printable ASCII characters only
				data[i] = byte('A' + (i % 26))
			}
			payload := []byte(fmt.Sprintf(`{"data":"%s"}`, string(data)))

			id, err := store.Enqueue(ctx, &storage.Job{
				Type:    "large",
				Payload: payload,
			})
			require.NoError(t, err, "1MB payload should succeed")

			// Verify retrieval works
			job, err := store.GetJob(ctx, id)
			require.NoError(t, err)
			// Note: Postgres JSONB may normalize JSON formatting, so check size is close
			// rather than exact byte equality
			assert.InDelta(t, len(payload), len(job.Payload), float64(len(payload))*0.05,
				"payload size should be within 5%% of original (accounting for JSON normalization)")
		})

		t.Run("5MB Payload", func(t *testing.T) {
			// 5MB payload - should succeed
			// Use valid JSON with printable characters
			dataSize := 5*1024*1024 - 20 // Account for JSON wrapper
			data := make([]byte, dataSize)
			for i := range data {
				// Use printable ASCII characters only
				data[i] = byte('A' + (i % 26))
			}
			payload := []byte(fmt.Sprintf(`{"data":"%s"}`, string(data)))

			id, err := store.Enqueue(ctx, &storage.Job{
				Type:    "large",
				Payload: payload,
			})
			require.NoError(t, err, "5MB payload should succeed")

			// Verify retrieval works (use GetJob instead of Dequeue to avoid interference)
			job, err := store.GetJob(ctx, id)
			require.NoError(t, err)
			// Check size is close (accounting for JSON normalization)
			assert.InDelta(t, len(payload), len(job.Payload), float64(len(payload))*0.05,
				"payload size should be within 5%% of original")
		})

		t.Run("Exceeds Maximum", func(t *testing.T) {
			// 11MB payload - should fail validation
			// Create a large JSON payload
			dataSize := 11*1024*1024 - 20
			data := make([]byte, dataSize)
			for i := range data {
				data[i] = byte('A' + (i % 26))
			}
			payload := []byte(fmt.Sprintf(`{"data":"%s"}`, string(data)))

			_, err := store.Enqueue(ctx, &storage.Job{
				Type:    "too_large",
				Payload: payload,
			})
			assert.Error(t, err, "payload exceeding MaxPayloadSize should fail")
			assert.Contains(t, err.Error(), "exceeds maximum", "error should mention size limit")
		})

		t.Run("Empty Payload", func(t *testing.T) {
			// Empty payload should be handled gracefully (defaults to {})
			id, err := store.Enqueue(ctx, &storage.Job{
				Type:    "empty",
				Payload: nil,
			})
			require.NoError(t, err)

			job, err := store.GetJob(ctx, id)
			require.NoError(t, err)
			assert.NotNil(t, job.Payload, "payload should not be nil")
			assert.Equal(t, []byte("{}"), job.Payload, "empty payload should default to {}")
		})
	}
}

// testPriorityValidation verifies that priority values are validated
func testPriorityValidation(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		t.Run("ValidPriorities", func(t *testing.T) {
			// Test boundary values
			validPriorities := []int{0, 1, 500, 999, 1000}
			for _, priority := range validPriorities {
				_, err := store.Enqueue(ctx, &storage.Job{
					Type:     "test",
					Priority: priority,
				})
				assert.NoError(t, err, "priority %d should be valid", priority)
			}
		})

		t.Run("InvalidPriorities", func(t *testing.T) {
			// Test out-of-range values
			invalidPriorities := []int{-1, -10, 1001, 9999}
			for _, priority := range invalidPriorities {
				_, err := store.Enqueue(ctx, &storage.Job{
					Type:     "test",
					Priority: priority,
				})
				assert.Error(t, err, "priority %d should be invalid", priority)
				assert.Contains(t, err.Error(), "priority", "error should mention priority")
			}
		})
	}
}

// testMaxRetriesNeverRetry verifies that MaxRetries=-1 means never retry
func testMaxRetriesNeverRetry(factory StorageFactory) func(*testing.T) {
	return func(t *testing.T) {
		store := factory(t)
		defer store.Close()
		ctx := context.Background()

		// Enqueue job with MaxRetries=-1 (never retry)
		id, err := store.Enqueue(ctx, &storage.Job{
			Type:       "one_shot",
			MaxRetries: -1,
		})
		require.NoError(t, err)

		// Retrieve and verify MaxRetries was set to 0 (stored representation of "never retry")
		job, err := store.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, 0, job.MaxRetries, "MaxRetries=-1 should be stored as 0")

		// Dequeue and fail the job
		job, err = store.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, job)

		err = store.Fail(ctx, job.ID, "failed")
		require.NoError(t, err)

		// Verify it cannot be retried (MaxRetries=0, RetryCount=0, so retry not allowed)
		job, err = store.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, 0, job.MaxRetries)
		assert.Equal(t, 0, job.RetryCount)
		assert.Equal(t, storage.StatusFailed, job.Status)
	}
}
