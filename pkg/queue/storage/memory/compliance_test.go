package memory_test

import (
	"testing"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
	"github.com/andreademasi/goku/pkg/queue/storage/tests"
)

// TestMemoryCompliance runs the complete compliance test suite against the in-memory backend.
// This ensures the in-memory implementation meets all Storage interface requirements.
func TestMemoryCompliance(t *testing.T) {
	tests.RunCompliance(t, func(t *testing.T) storage.Storage {
		return memory.New()
	})
}
