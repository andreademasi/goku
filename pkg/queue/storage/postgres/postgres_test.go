package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/andreademasi/goku/pkg/queue/storage"
	pgstorage "github.com/andreademasi/goku/pkg/queue/storage/postgres"
)

const (
	postgresUser     = "postgres"
	postgresPassword = "postgres"
	postgresDB       = "goqueue"
)

func newTestStore(t *testing.T) (*pgstorage.PostgresStorage, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)

	container, err := postgrescontainer.RunContainer(ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		postgrescontainer.WithUsername(postgresUser),
		postgrescontainer.WithPassword(postgresPassword),
		postgrescontainer.WithDatabase(postgresDB),
	)
	require.NoError(t, err)

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return db.PingContext(ctx) == nil
	}, 30*time.Second, 500*time.Millisecond, "postgres container did not become ready in time")

	require.NoError(t, pgstorage.InitSchema(ctx, db))

	store, err := pgstorage.New(db)
	require.NoError(t, err)

	cleanup := func() {
		cancel()
		require.NoError(t, store.Close())
		require.NoError(t, container.Terminate(context.Background()))
	}

	return store, cleanup
}

func TestPostgresStorageEnqueueDequeueComplete(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	ctx := context.Background()

	job := &storage.Job{
		Type:       "email",
		Payload:    []byte(`{"message":"hello"}`),
		Priority:   5,
		MaxRetries: 3,
	}

	id, err := store.Enqueue(ctx, job)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	dequeued, err := store.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	require.Equal(t, id, dequeued.ID)
	require.Equal(t, storage.StatusRunning, dequeued.Status)

	require.NoError(t, store.Complete(ctx, dequeued.ID))

	completed, err := store.GetJob(ctx, dequeued.ID)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, completed.Status)
	require.NotNil(t, completed.CompletedAt)
	require.Nil(t, completed.Error)
}

func TestPostgresStorageFailAndRetry(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	ctx := context.Background()

	job := &storage.Job{Type: "sync"}
	_, err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	dequeued, err := store.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	const failMessage = "temporary failure"
	require.NoError(t, store.Fail(ctx, dequeued.ID, failMessage))

	failed, err := store.GetJob(ctx, dequeued.ID)
	require.NoError(t, err)
	require.Equal(t, storage.StatusFailed, failed.Status)
	require.NotNil(t, failed.Error)
	require.Equal(t, failMessage, *failed.Error)

	require.NoError(t, store.Retry(ctx, dequeued.ID))

	retried, err := store.GetJob(ctx, dequeued.ID)
	require.NoError(t, err)
	require.Equal(t, storage.StatusPending, retried.Status)
	require.Equal(t, 1, retried.RetryCount)
	require.Nil(t, retried.Error)
	require.Nil(t, retried.ClaimedAt)
	require.Nil(t, retried.CompletedAt)
}

func TestPostgresStorageTransactionalEnqueue(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	ctx := context.Background()

	tx, err := store.BeginTx(ctx)
	require.NoError(t, err)

	job := &storage.Job{Type: "tx"}
	id, err := tx.Enqueue(ctx, job)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	require.NoError(t, tx.Rollback())

	_, err = store.GetJob(ctx, id)
	require.ErrorIs(t, err, storage.ErrJobNotFound)

	committedTx, err := store.BeginTx(ctx)
	require.NoError(t, err)

	committedID, err := committedTx.Enqueue(ctx, &storage.Job{Type: "tx"})
	require.NoError(t, err)
	require.NoError(t, committedTx.Commit())

	storedJob, err := store.GetJob(ctx, committedID)
	require.NoError(t, err)
	require.Equal(t, storage.StatusPending, storedJob.Status)
}

func TestPostgresStorageConcurrentDequeue(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	ctx := context.Background()

	const jobCount = 10
	for i := 0; i < jobCount; i++ {
		_, err := store.Enqueue(ctx, &storage.Job{Type: fmt.Sprintf("batch-%d", i)})
		require.NoError(t, err)
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		ids   = make(map[string]struct{})
		errCh = make(chan error, jobCount)
	)

	for i := 0; i < jobCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			job, err := store.Dequeue(ctx)
			if err != nil {
				errCh <- err
				return
			}

			if job == nil {
				errCh <- fmt.Errorf("expected job, got nil")
				return
			}

			mu.Lock()
			if _, exists := ids[job.ID]; exists {
				errCh <- fmt.Errorf("duplicate job dequeued: %s", job.ID)
			} else {
				ids[job.ID] = struct{}{}
			}
			mu.Unlock()

			if err := store.Complete(context.Background(), job.ID); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	require.Len(t, ids, jobCount)

	empty, err := store.Dequeue(ctx)
	require.NoError(t, err)
	require.Nil(t, empty)
}
