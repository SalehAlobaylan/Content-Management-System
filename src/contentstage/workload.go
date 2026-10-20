package contentstage

import (
	"fmt"

	"content-management-system/src/models"
	"content-management-system/src/workload"

	"gorm.io/gorm"
)

const workloadOwner = "content_stage"

// Model and FFmpeg capacity follows the current durable stage owner. The CMS
// classifier is a deterministic projection and never consumes a model permit.
func stageWorkloadClass(stage string) string {
	switch stage {
	case models.ContentStageNewsTextEmbedding, models.ContentStagePodsTextEmbedding, models.ContentStagePodsCaptionReembedding:
		return "text_embedding"
	case models.ContentStageNewsLLMMetadata, models.ContentStagePodsLLMMetadata:
		return "llm_batch"
	case models.ContentStagePodsTranscript:
		return "stt_batch"
	case models.ContentStagePodsImageEmbedding:
		return "image_embedding"
	case models.ContentStagePodsAtomization:
		return "atomize"
	case models.ContentStagePodsMediaArtifacts:
		return "pods_stage"
	default:
		return ""
	}
}

func stageWorkloadRequest(request models.ContentStageRequest) workload.Request {
	return workload.Request{
		TenantID: request.TenantID, System: "content_stages", Class: stageWorkloadClass(request.Stage),
		Units: 1, RunID: request.PublicID.String(), Origin: workload.OriginAutomation,
		OwnerKind: workloadOwner, OwnerID: request.PublicID.String(),
		RequestKey: fmt.Sprintf("stage:%s:%d", request.PublicID, request.ClaimEpoch+1),
	}
}

func activeStageWorkloadRequest(request models.ContentStageRequest) workload.Request {
	r := stageWorkloadRequest(request)
	r.RequestKey = fmt.Sprintf("stage:%s:%d", request.PublicID, request.ClaimEpoch)
	return r
}

func settleStageWorkload(tx *gorm.DB, request models.ContentStageRequest, units int, proof string) {
	// Receipt processing remains available when advisory capacity storage fails.
	// The governor isolates its write from the native owner transaction.
	_ = workload.SettleRequest(tx, activeStageWorkloadRequest(request), units, proof)
}

func settleUnstartedStageWorkload(tx *gorm.DB, request models.ContentStageRequest, proof string) {
	var started int64
	if err := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND request_id=? AND effect_started_at IS NOT NULL", request.TenantID, request.PublicID).Count(&started).Error; err == nil && started == 0 {
		settleStageWorkload(tx, request, 0, proof)
	}
}

func settleVerifiedStageWorkload(tx *gorm.DB, request models.ContentStageRequest) {
	var latest models.ContentStageAttempt
	if err := tx.Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Order("attempt_number DESC").First(&latest).Error; err != nil || latest.State != models.ContentStageVerified {
		return
	}
	var receipts []models.ContentStageReceipt
	if err := tx.Where("tenant_id=? AND request_id=? AND attempt_id=?", request.TenantID, request.PublicID, latest.PublicID).Find(&receipts).Error; err != nil {
		return
	}
	for _, receipt := range receipts {
		if matchingAttemptReceipt(latest, request, receipt) {
			settleStageWorkload(tx, request, 1, "stage_exact_attempt_artifact_and_receipt_verified")
			return
		}
	}
}
