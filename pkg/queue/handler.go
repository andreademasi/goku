package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/andreademasi/goku/pkg/queue/storage"
)

// Handler processes a job payload.
type Handler func(ctx context.Context, payload []byte) error

// HandlerFunc provides type-safe job handling using generics.
type HandlerFunc[T any] func(ctx context.Context, job T) error

// ToHandler converts a typed HandlerFunc to a generic Handler.
func (h HandlerFunc[T]) ToHandler() Handler {
	return func(ctx context.Context, payload []byte) error {
		var job T
		if err := json.Unmarshal(payload, &job); err != nil {
			return fmt.Errorf("failed to unmarshal job payload: %w", err)
		}
		return h(ctx, job)
	}
}

type handlerRegistry struct {
	handlers map[string]Handler
}

func newHandlerRegistry() *handlerRegistry {
	return &handlerRegistry{
		handlers: make(map[string]Handler),
	}
}

func (r *handlerRegistry) register(jobType string, handler Handler) error {
	if _, exists := r.handlers[jobType]; exists {
		return fmt.Errorf("%w: %s", ErrHandlerAlreadyRegistered, jobType)
	}
	r.handlers[jobType] = handler
	return nil
}

func (r *handlerRegistry) get(jobType string) (Handler, error) {
	handler, exists := r.handlers[jobType]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrHandlerNotFound, jobType)
	}
	return handler, nil
}

func (r *handlerRegistry) has(jobType string) bool {
	_, exists := r.handlers[jobType]
	return exists
}

func (r *handlerRegistry) types() []string {
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

func (r *handlerRegistry) processJob(ctx context.Context, job *storage.Job) error {
	handler, err := r.get(job.Type)
	if err != nil {
		return err
	}
	return handler(ctx, job.Payload)
}
