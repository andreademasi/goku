package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/andreademasi/goku/pkg/queue"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ZerologAdapter struct {
	logger zerolog.Logger
}

func NewZerologAdapter(logger zerolog.Logger) *ZerologAdapter {
	return &ZerologAdapter{logger: logger}
}

func (a *ZerologAdapter) Debug(msg string, keysAndValues ...any) {
	event := a.logger.Debug()
	a.addFields(event, keysAndValues)
	event.Msg(msg)
}

func (a *ZerologAdapter) Info(msg string, keysAndValues ...any) {
	event := a.logger.Info()
	a.addFields(event, keysAndValues)
	event.Msg(msg)
}

func (a *ZerologAdapter) Warn(msg string, keysAndValues ...any) {
	event := a.logger.Warn()
	a.addFields(event, keysAndValues)
	event.Msg(msg)
}

func (a *ZerologAdapter) Error(msg string, keysAndValues ...any) {
	event := a.logger.Error()
	a.addFields(event, keysAndValues)
	event.Msg(msg)
}

func (a *ZerologAdapter) addFields(event *zerolog.Event, keysAndValues []any) {
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := fmt.Sprintf("%v", keysAndValues[i])
			event.Interface(key, keysAndValues[i+1])
		}
	}
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	gokuLogger := NewZerologAdapter(log.Logger)

	store := memory.New()

	q, err := queue.New(queue.Config{
		Storage: store,
		Workers: 3,
		Logger:  gokuLogger,
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to create queue")
		os.Exit(1)
	}

	q.Register("SendEmail", queue.HandlerFunc[EmailJob](
		func(ctx context.Context, job EmailJob) error {
			log.Info().
				Str("to", job.To).
				Str("subject", job.Subject).
				Msg("sending email")
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	).ToHandler())

	if err := q.Start(); err != nil {
		log.Error().Err(err).Msg("failed to start queue")
		os.Exit(1)
	}
	defer q.Stop()

	log.Info().Int("workers", 3).Msg("queue started")

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		jobID, err := q.Enqueue(ctx, "SendEmail", EmailJob{
			To:      "user@example.com",
			Subject: "Test Email",
			Body:    "This is a test email",
		})
		if err != nil {
			log.Error().Err(err).Msg("failed to enqueue job")
			continue
		}
		log.Info().Str("job_id", jobID).Msg("job enqueued")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	log.Info().Msg("shutting down")
}
