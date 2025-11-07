# Goku

A job queue library for Go with pluggable storage backends.

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **⚠️ Early Development**: This project is in active development and APIs may change. Not recommended for production use yet.

## Features

- Pluggable storage backends (Postgres, SQLite, in-memory)
- Type-safe job handlers using Go generics
- Concurrent worker pools
- Configurable retry strategies
- Priority-based job processing
- Scheduled/delayed jobs
- Graceful shutdown
- Automatic cleanup of old jobs

## Quick Start

### Installation

```bash
go get github.com/andreademasi/goku
```

### Basic Usage

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/andreademasi/goku/pkg/queue"
    "github.com/andreademasi/goku/pkg/queue/storage/sqlite"
)

// Define your job type
type EmailJob struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

func main() {
    // 1. Create storage backend
    store, err := sqlite.New("jobs.db")
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Initialize database schema
    ctx := context.Background()
    store.InitSchema(ctx)

    // 2. Create queue
    q, err := queue.New(queue.Config{
        Storage: store,
        Workers: 5,
    })
    if err != nil {
        log.Fatal(err)
    }

    // 3. Register job handlers (type-safe!)
    q.Register("SendEmail", queue.HandlerFunc[EmailJob](func(ctx context.Context, job EmailJob) error {
        // Process the job
        log.Printf("Sending email to %s: %s", job.To, job.Subject)
        // ... send email logic ...
        return nil
    }).ToHandler())

    // 4. Start processing
    if err := q.Start(); err != nil {
        log.Fatal(err)
    }
    defer q.Stop()

    // 5. Enqueue jobs
    jobID, err := q.Enqueue(ctx, "SendEmail", EmailJob{
        To:      "user@example.com",
        Subject: "Welcome!",
        Body:    "Thanks for signing up",
    })
    log.Printf("Job enqueued: %s", jobID)

    // Keep running...
    select {}
}
```

## Storage Backends

### SQLite

```go
import "github.com/andreademasi/goku/pkg/queue/storage/sqlite"

store, err := sqlite.New("jobs.db")
```

Good for single-server applications and development. Supports ACID transactions.

### PostgreSQL

```go
import (
    "database/sql"
    "github.com/andreademasi/goku/pkg/queue/storage/postgres"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres", "postgresql://user:pass@localhost/mydb")
store, err := postgres.New(db)
store.InitSchema(ctx)
```

Suitable for multi-server setups. Supports distributed workers and transactions.

### In-Memory

```go
import "github.com/andreademasi/goku/pkg/queue/storage/memory"

store := memory.New()
```

Fast and simple, but no persistence. Useful for testing.

## Advanced Usage

### Priority Jobs

Process important jobs first:

```go
q.EnqueueWithOptions(ctx, "SendEmail", job, queue.EnqueueOptions{
    Priority: 100, // Higher = more important
})
```

Jobs are processed in priority order (100 before 50 before 10).

### Scheduled Jobs

Delay execution until a specific time:

```go
q.EnqueueWithOptions(ctx, "SendReport", report, queue.EnqueueOptions{
    ScheduledAt: time.Now().Add(24 * time.Hour), // Run tomorrow
})
```

### Custom Retry Strategies

Configure how failed jobs are retried:

```go
q, err := queue.New(queue.Config{
    Storage: store,
    Workers: 5,
    RetryStrategy: queue.ExponentialBackoffWithJitter{
        BaseDelay: 1 * time.Second,
        MaxDelay:  1 * time.Hour,
    },
})
```

**Available strategies:**

- `ExponentialBackoff` - 1s, 2s, 4s, 8s, 16s, ...
- `ExponentialBackoffWithJitter` - Adds randomness to prevent thundering herd
- `LinearBackoff` - 5s, 10s, 15s, 20s, ...
- `ConstantBackoff` - Same delay every time

### Transactional Enqueuing

SQL backends support transactional job enqueuing:

```go
queueTx, err := store.(storage.TransactionalStorage).BeginTx(ctx)
defer queueTx.Rollback()

queueTx.Enqueue(ctx, &storage.Job{
    Type:    "SendEmail",
    Payload: payload,
})

queueTx.Commit()
```

### Graceful Shutdown

Workers complete current jobs before stopping:

```go
// Start queue
q.Start()

// Wait for signal
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, os.Interrupt)
<-sigCh

// Graceful shutdown (waits up to ShutdownTimeout)
if err := q.Stop(); err != nil {
    log.Printf("Shutdown error: %v", err)
}
```

### Custom Logger

Integrate with your logging infrastructure:

```go
type MyLogger struct {
    logger *slog.Logger
}

func (l *MyLogger) Debug(msg string, keysAndValues ...interface{}) {
    l.logger.Debug(msg, keysAndValues...)
}

func (l *MyLogger) Info(msg string, keysAndValues ...interface{}) {
    l.logger.Info(msg, keysAndValues...)
}

func (l *MyLogger) Error(msg string, keysAndValues ...interface{}) {
    l.logger.Error(msg, keysAndValues...)
}

q, err := queue.New(queue.Config{
    Storage: store,
    Workers: 5,
    Logger:  &MyLogger{logger: slog.Default()},
})
```

### Job Cleanup

Automatically remove old jobs to prevent database growth:

```go
q, err := queue.New(queue.Config{
    Storage: store,
    Workers: 5,
    CleanupInterval: 1 * time.Hour,
    CleanupAge: 24 * time.Hour,
})
```

### Stale Job Recovery

Recover jobs stuck in "running" state after worker crashes:

```go
q, err := queue.New(queue.Config{
    Storage: store,
    Workers: 5,
    StaleJobTimeout: 5 * time.Minute,
})
```

## Configuration

```go
config := queue.Config{
    Storage:         store,
    Workers:         10,
    PollInterval:    500 * time.Millisecond,
    ShutdownTimeout: 60 * time.Second,
    RetryStrategy:   queue.ExponentialBackoff{},
    Logger:          myLogger,
    CleanupInterval: 1 * time.Hour,
    CleanupAge:      24 * time.Hour,
    StaleJobTimeout: 5 * time.Minute,
}
```

## Examples

See the `cmd/example/` directory for working examples:

```bash
go run ./cmd/example/queue
```

## Architecture

The library is structured in three layers:

1. **Queue API** - High-level interface with type-safe handlers and worker management
2. **Storage Interface** - Backend-agnostic abstraction layer
3. **Storage Implementations** - Postgres, SQLite, and in-memory backends

This makes it easy to swap storage backends without changing application code.

## Testing

Run tests:

```bash
go test ./...
```

Run with race detector:

```bash
go test -race ./...
```

## Similar Projects

If you're evaluating job queue libraries, also check out:

- [River](https://github.com/riverqueue/river) - Postgres-backed queue with robust features
- [Asynq](https://github.com/hibiken/asynq) - Redis-backed queue, battle-tested
- [Machinery](https://github.com/RichardKnop/machinery) - Multi-backend queue

Goku focuses on pluggable storage backends and type-safe handlers using generics.

## License

MIT License - see [LICENSE](LICENSE) file for details.
