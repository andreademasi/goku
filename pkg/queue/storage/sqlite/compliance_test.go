package sqlite

import (
	"context"
	"testing"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/tests"
	"github.com/stretchr/testify/require"
)

// TestSQLiteCompliance runs the complete compliance test suite against SQLite storage.
// All storage implementations must pass these tests to ensure consistent behavior.
func TestSQLiteCompliance(t *testing.T) {
	tests.RunCompliance(t, func(t *testing.T) storage.Storage {
		// Use in-memory database for tests (fast, isolated)
		store, err := New(":memory:")
		require.NoError(t, err, "failed to create SQLite storage")

		// Initialize schema
		err = store.InitSchema(context.Background())
		require.NoError(t, err, "failed to initialize schema")

		return store
	})
}
