package controllers

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestCreatePersistedTranscriptionBatchRejectsOverCapBeforeDatabaseWork(t *testing.T) {
	ids := make([]string, maxTranscriptionBatchItems+1)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	if _, _, err := createPersistedTranscriptionBatch(nil, "default", "admin", ids, true); err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("over-cap batch error = %v, want cap rejection", err)
	}
}

func TestCreatePersistedTranscriptionBatchRejectsDuplicateBeforeDatabaseWork(t *testing.T) {
	id := uuid.NewString()
	if _, _, err := createPersistedTranscriptionBatch(nil, "default", "admin", []string{id, id}, true); err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("duplicate batch error = %v, want duplicate rejection", err)
	}
}
