package sqlite

import (
	"context"
	"fmt"
)

// InitSchema creates the jobs table and indexes if they don't already exist.
//
// This method is idempotent and safe to call multiple times.
// It should be called once during application initialization.
//
// The schema includes:
// - jobs table with all required columns
// - Indexes for fast job selection by status and priority
// - Indexes for filtering by type and completion time
func (s *SQLiteStorage) InitSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		type TEXT NOT NULL,
		payload TEXT NOT NULL,
		status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'completed', 'failed')),
		priority INTEGER NOT NULL DEFAULT 0,
		max_retries INTEGER NOT NULL DEFAULT 3,
		retry_count INTEGER NOT NULL DEFAULT 0,
		scheduled_at TEXT NOT NULL DEFAULT (datetime('now')),
		claimed_at TEXT,
		completed_at TEXT,
		error TEXT,
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	);

	-- Index for fast job selection (critical for Dequeue performance)
	CREATE INDEX IF NOT EXISTS idx_jobs_pending 
	ON jobs(status, priority DESC, scheduled_at)
	WHERE status = 'pending';

	-- Index for job type filtering
	CREATE INDEX IF NOT EXISTS idx_jobs_type 
	ON jobs(type);

	-- Index for cleanup queries (finding old completed/failed jobs)
	CREATE INDEX IF NOT EXISTS idx_jobs_completed
	ON jobs(completed_at)
	WHERE status IN ('completed', 'failed');
	`

	_, err := s.db.ExecContext(ctx, schema)
	if err != nil {
		return fmt.Errorf("sqlite: failed to initialize schema: %w", err)
	}

	return nil
}

// DropSchema removes all tables and indexes.
//
// WARNING: This will permanently delete all job data.
// This method is primarily for testing and should not be used in production.
func (s *SQLiteStorage) DropSchema(ctx context.Context) error {
	drops := []string{
		"DROP INDEX IF EXISTS idx_jobs_pending",
		"DROP INDEX IF EXISTS idx_jobs_type",
		"DROP INDEX IF EXISTS idx_jobs_completed",
		"DROP TABLE IF EXISTS jobs",
	}

	for _, drop := range drops {
		if _, err := s.db.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf("sqlite: failed to drop schema: %w", err)
		}
	}

	return nil
}

// Vacuum reclaims unused space and optimizes the database file.
//
// This should be run periodically (e.g., after deleting many jobs) to:
// - Reduce database file size
// - Improve query performance
// - Defragment the database
//
// Note: VACUUM can take a while on large databases and locks the database exclusively.
func (s *SQLiteStorage) Vacuum(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "VACUUM")
	if err != nil {
		return fmt.Errorf("sqlite: vacuum failed: %w", err)
	}
	return nil
}

// Analyze updates the query optimizer statistics.
//
// This should be run periodically to help SQLite choose optimal query plans.
// It's particularly useful after bulk inserts or deletions.
func (s *SQLiteStorage) Analyze(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "ANALYZE")
	if err != nil {
		return fmt.Errorf("sqlite: analyze failed: %w", err)
	}
	return nil
}
