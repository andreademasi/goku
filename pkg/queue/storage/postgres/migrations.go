package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'completed', 'failed')),
    priority INTEGER NOT NULL DEFAULT 0 CHECK(priority >= 0 AND priority <= 1000),
    max_retries INTEGER NOT NULL DEFAULT 3 CHECK(max_retries >= 0),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_pending
ON jobs(status, priority DESC, scheduled_at)
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_jobs_type
ON jobs(type);
`

// InitSchema creates the jobs table and required indexes if they do not already exist.
func InitSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("postgres: init schema: db is nil")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("postgres: init schema exec: %w", err)
	}

	return nil
}
