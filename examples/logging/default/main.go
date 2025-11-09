package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"log/slog"

	"github.com/andreademasi/goku/pkg/queue"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
)

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	// Use default noop logger (no logs)
	store := memory.New()

	q, err := queue.New(queue.Config{
		Storage: store,
		Workers: 3,
	})
	if err != nil {
		slog.Error("failed to create queue", "error", err)
		os.Exit(1)
	}

	q.Register("SendEmail", queue.HandlerFunc[EmailJob](
		func(ctx context.Context, job EmailJob) error {
			slog.Info("sending email",
				slog.String("to", job.To),
				slog.String("subject", job.Subject))
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	).ToHandler())

	if err := q.Start(); err != nil {
		slog.Error("failed to start queue", "error", err)
		os.Exit(1)
	}
	defer q.Stop()

	slog.Info("queue started", slog.Int("workers", 3))

	ctx := context.Background()
	for range 5 {
		jobID, err := q.Enqueue(ctx, "SendEmail", EmailJob{
			To:      "user@example.com",
			Subject: "Test Email",
			Body:    "This is a test email",
		})
		if err != nil {
			slog.Error("failed to enqueue job", "error", err)
			continue
		}
		slog.Info("job enqueued", slog.String("job_id", jobID))
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	slog.Info("shutting down")
}
