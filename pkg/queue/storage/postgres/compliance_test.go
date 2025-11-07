package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/andreademasi/goku/pkg/queue/storage"
	pgstorage "github.com/andreademasi/goku/pkg/queue/storage/postgres"
	"github.com/andreademasi/goku/pkg/queue/storage/tests"
)

// TestPostgresCompliance runs the complete compliance test suite against the Postgres backend.
// This ensures Postgres implements all Storage interface requirements correctly.
func TestPostgresCompliance(t *testing.T) {
	tests.RunCompliance(t, newTestStoreForCompliance)
}

// newTestStoreForCompliance creates a fresh Postgres storage instance for each compliance test.
// Each test gets its own isolated database container.
func newTestStoreForCompliance(t *testing.T) storage.Storage {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// Start Postgres container
	container, err := postgrescontainer.RunContainer(ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		postgrescontainer.WithUsername(postgresUser),
		postgrescontainer.WithPassword(postgresPassword),
		postgrescontainer.WithDatabase(postgresDB),
	)
	require.NoError(t, err)

	// Cleanup container when test finishes
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	// Connect to database
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)

	// Wait for database to be ready
	require.Eventually(t, func() bool {
		return db.PingContext(ctx) == nil
	}, 30*time.Second, 500*time.Millisecond, "postgres container did not become ready in time")

	// Initialize schema
	require.NoError(t, pgstorage.InitSchema(ctx, db))

	// Create storage instance
	store, err := pgstorage.New(db)
	require.NoError(t, err)

	return store
}
