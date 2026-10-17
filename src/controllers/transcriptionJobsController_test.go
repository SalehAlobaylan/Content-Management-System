package controllers

import (
	"content-management-system/src/models"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestExplicitTranscriptionAdmissionKeepsBudgetGuardWhenAutomaticSTTIsOff(t *testing.T) {
	media, duration := "https://example.test/episode.mp3", 861
	item := &models.ContentItem{MediaURL: &media, DurationSec: &duration}
	cfg := models.TranscriptionConfig{AutoSttEnabled: false}
	if allowed, reason := evaluateSTTAdmissionWithConfig(nil, item, models.TranscriptionTriggerManual, cfg); !allowed {
		t.Fatalf("explicit request rejected: %s", reason)
	}
	if allowed, _ := evaluateSTTAdmissionWithConfig(nil, item, models.TranscriptionTriggerIngestAuto, cfg); allowed {
		t.Fatal("automatic work must remain disabled")
	}
	cfg.MonthlyBudgetCapUsd = 0.001
	if allowed, reason := evaluateSTTAdmissionWithConfig(nil, item, models.TranscriptionTriggerManual, cfg); allowed || reason != "monthly STT budget cap reached" {
		t.Fatalf("manual request bypassed budget: allowed=%t reason=%s", allowed, reason)
	}
}

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
