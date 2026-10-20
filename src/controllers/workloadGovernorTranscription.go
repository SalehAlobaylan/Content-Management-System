package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/workload"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const legacySTTWorkloadOwner = "transcription_job"

func transcriptionWorkloadRequest(tenant string, id uuid.UUID, trigger string, trustedOrigins ...string) workload.Request {
	origin := workload.OriginAutomation
	if trigger == models.TranscriptionTriggerManual {
		origin = workload.OriginHuman
	}
	if trigger == models.TranscriptionTriggerBulkManual {
		origin = workload.OriginCampaign
	}
	// Capacity provenance is independent of STT's existing financial trigger.
	// Only CMS callers can provide this unexported argument.
	if len(trustedOrigins) > 0 {
		switch trustedOrigins[0] {
		case workload.OriginHuman, workload.OriginAutomation, workload.OriginCampaign:
			origin = trustedOrigins[0]
		}
	}
	return workload.Request{TenantID: tenant, System: "transcription_owner", Class: "stt_batch", Units: 1, RunID: id.String(), OwnerKind: legacySTTWorkloadOwner, OwnerID: id.String(), RequestKey: id.String(), Origin: origin}
}

// Only a trusted remote terminal receipt proves a possibly submitted legacy
// job stopped. Local HTTP errors, cancellation flags and TTL cannot settle it.
func settleLegacySTTReceipt(db *gorm.DB, job models.TranscriptionJob, reportedStatus *string) {
	if reportedStatus == nil || !terminalTranscriptionStatus(*reportedStatus) || job.CompletedAt == nil {
		return
	}
	actual := 1
	if job.Status == models.TranscriptionJobStatusSkipped {
		actual = 0
	}
	_ = workload.SettleOwner(db, job.TenantID, legacySTTWorkloadOwner, job.PublicID.String(), actual, "media_terminal_transcription_owner_receipt")
}
