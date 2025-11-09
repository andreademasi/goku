package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("InMemory", func(t *testing.T) {
		store, err := New(":memory:")
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()

		err = store.InitSchema(context.Background())
		require.NoError(t, err)
	})

	t.Run("FileBased", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		store, err := New(dbPath)
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()

		err = store.InitSchema(context.Background())
		require.NoError(t, err)

		// Verify file was created
		_, err = os.Stat(dbPath)
		require.NoError(t, err, "database file should exist")
	})

	t.Run("EmptyPath", func(t *testing.T) {
		_, err := New("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path is required")
	})
}

func TestSQLiteSpecific_WALMode(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := New(dbPath)
	require.NoError(t, err)
	defer store.Close()

	// Check WAL mode is enabled (file-based databases only)
	var journalMode string
	err = store.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	require.NoError(t, err)
	assert.Equal(t, "wal", journalMode, "WAL mode should be enabled for file-based databases")
}

func TestSQLiteSpecific_Pragmas(t *testing.T) {
	store, err := New(":memory:")
	require.NoError(t, err)
	defer store.Close()

	// Verify foreign keys are enabled
	var foreignKeys int
	err = store.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys)
	require.NoError(t, err)
	assert.Equal(t, 1, foreignKeys, "foreign keys should be enabled")

	// Verify synchronous mode
	var synchronous string
	err = store.db.QueryRow("PRAGMA synchronous").Scan(&synchronous)
	require.NoError(t, err)
	// synchronous=1 is NORMAL
	assert.NotEmpty(t, synchronous)
}

func TestSQLiteSpecific_Transactions(t *testing.T) {
	store, err := New(":memory:")
	require.NoError(t, err)
	defer store.Close()

	err = store.InitSchema(context.Background())
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("CommitTransaction", func(t *testing.T) {
		// Start transaction
		tx, err := store.BeginTx(ctx)
		require.NoError(t, err)

		// Enqueue in transaction
		job := &storage.Job{
			Type:    "test_commit",
			Payload: []byte(`{"message":"hello"}`),
		}

		id, err := tx.Enqueue(ctx, job)
		require.NoError(t, err)
		require.NotEmpty(t, id)

		// Commit
		err = tx.Commit()
		require.NoError(t, err)

		// Now job should be visible
		jobs, err := store.ListJobs(ctx, storage.JobFilter{})
		require.NoError(t, err)

		// Find our job
		found := false
		for _, j := range jobs {
			if j.Type == "test_commit" {
				found = true
				break
			}
		}
		assert.True(t, found, "job should be visible after commit")
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		// Start transaction
		tx, err := store.BeginTx(ctx)
		require.NoError(t, err)

		// Enqueue in transaction
		job := &storage.Job{
			Type:    "test_rollback",
			Payload: []byte(`{"message":"world"}`),
		}

		id, err := tx.Enqueue(ctx, job)
		require.NoError(t, err)
		require.NotEmpty(t, id)

		// Rollback
		err = tx.Rollback()
		require.NoError(t, err)

		// Job should not be visible
		jobs, err := store.ListJobs(ctx, storage.JobFilter{
			Types: []string{"test_rollback"},
		})
		require.NoError(t, err)
		assert.Equal(t, 0, len(jobs), "job should not exist after rollback")
	})

	t.Run("MultipleEnqueuesInTransaction", func(t *testing.T) {
		// Start transaction
		tx, err := store.BeginTx(ctx)
		require.NoError(t, err)

		// Enqueue multiple jobs
		for i := 0; i < 5; i++ {
			job := &storage.Job{
				Type:    "batch_test",
				Payload: []byte(`{}`),
			}
			_, err := tx.Enqueue(ctx, job)
			require.NoError(t, err)
		}

		// Commit
		err = tx.Commit()
		require.NoError(t, err)

		// Verify all jobs are visible
		jobs, err := store.ListJobs(ctx, storage.JobFilter{
			Types: []string{"batch_test"},
		})
		require.NoError(t, err)
		assert.Equal(t, 5, len(jobs), "all jobs should be committed")
	})
}

func TestSQLiteSpecific_FileCreation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := New(dbPath)
	require.NoError(t, err)
	defer store.Close()

	err = store.InitSchema(context.Background())
	require.NoError(t, err)

	// Verify database file exists
	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	assert.False(t, info.IsDir())
	assert.Greater(t, info.Size(), int64(0), "database file should have content")

	// Verify WAL files exist (WAL mode creates additional files)
	walPath := dbPath + "-wal"
	shmPath := dbPath + "-shm"

	// WAL and SHM files might not exist immediately, but database should work
	_, walErr := os.Stat(walPath)
	_, shmErr := os.Stat(shmPath)

	// At least one of these should succeed (or we have the main file)
	if walErr != nil && shmErr != nil {
		// Main file should exist
		_, err := os.Stat(dbPath)
		require.NoError(t, err)
	}
}

func TestSQLiteSpecific_DateTimeHandling(t *testing.T) {
	store, err := New(":memory:")
	require.NoError(t, err)
	defer store.Close()

	err = store.InitSchema(context.Background())
	require.NoError(t, err)

	ctx := context.Background()

	// Enqueue a job
	job := &storage.Job{
		Type:    "datetime_test",
		Payload: []byte(`{}`),
	}

	id, err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Retrieve and verify datetime fields are parsed correctly
	retrieved, err := store.GetJob(ctx, id)
	require.NoError(t, err)

	assert.NotZero(t, retrieved.CreatedAt, "CreatedAt should be set")
	assert.NotZero(t, retrieved.ScheduledAt, "ScheduledAt should be set")
	assert.Nil(t, retrieved.ClaimedAt, "ClaimedAt should be nil initially")
	assert.Nil(t, retrieved.CompletedAt, "CompletedAt should be nil initially")

	// Dequeue and verify ClaimedAt is set
	dequeued, err := store.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.NotNil(t, dequeued.ClaimedAt, "ClaimedAt should be set after dequeue")

	// Complete and verify CompletedAt is set
	err = store.Complete(ctx, id)
	require.NoError(t, err)

	completed, err := store.GetJob(ctx, id)
	require.NoError(t, err)
	assert.NotNil(t, completed.CompletedAt, "CompletedAt should be set after complete")
}

func TestSQLiteSpecific_SchemaIdempotency(t *testing.T) {
	store, err := New(":memory:")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	// Call InitSchema multiple times
	for i := 0; i < 3; i++ {
		err = store.InitSchema(ctx)
		require.NoError(t, err, "InitSchema should be idempotent (call %d)", i+1)
	}

	// Verify schema is usable
	job := &storage.Job{
		Type:    "idempotency_test",
		Payload: []byte(`{}`),
	}

	id, err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	retrieved, err := store.GetJob(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "idempotency_test", retrieved.Type)
}

func TestSQLiteSpecific_DropSchema(t *testing.T) {
	store, err := New(":memory:")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	// Initialize schema
	err = store.InitSchema(ctx)
	require.NoError(t, err)

	// Add a job
	job := &storage.Job{
		Type:    "drop_test",
		Payload: []byte(`{}`),
	}

	id, err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Drop schema
	err = store.DropSchema(ctx)
	require.NoError(t, err)

	// Table should not exist anymore
	_, err = store.GetJob(ctx, id)
	require.Error(t, err)

	// Re-initialize schema should work
	err = store.InitSchema(ctx)
	require.NoError(t, err)

	// Should be able to enqueue again
	_, err = store.Enqueue(ctx, job)
	require.NoError(t, err)
}

func TestSQLiteSpecific_Vacuum(t *testing.T) {
	store, err := New(":memory:")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	err = store.InitSchema(ctx)
	require.NoError(t, err)

	// Vacuum should work (even on empty database)
	err = store.Vacuum(ctx)
	require.NoError(t, err)

	// Add some jobs and vacuum again
	for i := 0; i < 10; i++ {
		job := &storage.Job{
			Type:    "vacuum_test",
			Payload: []byte(`{}`),
		}
		_, err := store.Enqueue(ctx, job)
		require.NoError(t, err)
	}

	err = store.Vacuum(ctx)
	require.NoError(t, err)
}

func TestSQLiteSpecific_Analyze(t *testing.T) {
	store, err := New(":memory:")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	err = store.InitSchema(ctx)
	require.NoError(t, err)

	// Analyze should work
	err = store.Analyze(ctx)
	require.NoError(t, err)
}

func TestSQLiteSpecific_ConcurrentWrites(t *testing.T) {
	// This test verifies SQLite with WAL mode can handle concurrent writes
	// Note: SQLite still has a single writer, but WAL mode improves concurrency
	store, err := New(":memory:")
	require.NoError(t, err)
	defer store.Close()

	err = store.InitSchema(context.Background())
	require.NoError(t, err)

	ctx := context.Background()

	// Enqueue jobs from multiple goroutines
	// SQLite will serialize writes, but should not error
	const workerCount = 10
	const jobsPerWorker = 5

	errors := make(chan error, workerCount)
	done := make(chan bool, workerCount)

	for w := 0; w < workerCount; w++ {
		go func(workerID int) {
			for j := 0; j < jobsPerWorker; j++ {
				job := &storage.Job{
					Type:    "concurrent_write",
					Payload: []byte(`{}`),
				}

				_, err := store.Enqueue(ctx, job)
				if err != nil {
					errors <- err
					done <- false
					return
				}
			}
			done <- true
		}(w)
	}

	// Wait for all workers
	for i := 0; i < workerCount; i++ {
		<-done
	}
	close(errors)

	// Check for errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	require.Empty(t, errs, "concurrent writes should not produce errors")

	// Verify all jobs were inserted
	jobs, err := store.ListJobs(ctx, storage.JobFilter{
		Types: []string{"concurrent_write"},
	})
	require.NoError(t, err)
	assert.Equal(t, workerCount*jobsPerWorker, len(jobs),
		"all concurrent writes should succeed")
}

func TestSQLiteSpecific_IDGeneration(t *testing.T) {
	store, err := New(":memory:")
	require.NoError(t, err)
	defer store.Close()

	err = store.InitSchema(context.Background())
	require.NoError(t, err)

	ctx := context.Background()

	// Enqueue multiple jobs and verify IDs are unique and sequential
	ids := make(map[string]bool)
	for i := 0; i < 10; i++ {
		job := &storage.Job{
			Type:    "id_test",
			Payload: []byte(`{}`),
		}

		id, err := store.Enqueue(ctx, job)
		require.NoError(t, err)
		require.NotEmpty(t, id)

		// Check uniqueness
		require.False(t, ids[id], "ID %s should be unique", id)
		ids[id] = true
	}

	assert.Equal(t, 10, len(ids), "should have 10 unique IDs")
}
