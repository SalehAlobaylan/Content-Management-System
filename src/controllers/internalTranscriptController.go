package controllers

import (
	"content-management-system/src/artifacts"
	"content-management-system/src/contentstage"
	"content-management-system/src/models"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type internalCreateTranscriptRequest struct {
	ContentItemID  string                   `json:"content_item_id"`
	FullText       string                   `json:"full_text"`
	Summary        *string                  `json:"summary"`
	WordTimestamps []map[string]interface{} `json:"word_timestamps"`
	// Caption-first additions (all optional for backward compat):
	Segments            []map[string]interface{}            `json:"segments"` // [{start,end,text}]
	Chapters            []map[string]interface{}            `json:"chapters"` // [{start,end,title,source}]
	Source              *string                             `json:"source"`   // youtube_human|youtube_auto|stt_deepgram|stt_whisper
	Provider            *string                             `json:"provider"` // concrete engine name
	Language            *string                             `json:"language"`
	TranscriptionJobID  *string                             `json:"transcription_job_id"`
	LanguageProbability *float64                            `json:"language_probability"`
	DurationSec         *float64                            `json:"duration_sec"`
	ArtifactRecovery    *artifactRecoveryCorrelationRequest `json:"artifact_recovery,omitempty"`
	ContentStage        *contentStageCorrelationRequest     `json:"content_stage,omitempty"`
}

type internalCreateTranscriptResponse struct {
	ID        string `json:"id"`
	CreatedAt string `json:"created_at"`
}

// InternalCreateTranscript handles POST /internal/transcripts.
//
// Beyond creating the transcript row it now (a) persists segments/chapters/source/
// provider, (b) links the transcript to its content item and sets the lightweight
// caption_state + transcript_source in one pass, and (c) fires the model-agnostic
// re-enrichment cascade when this is an STT upgrade so search reflects the new text.
func InternalCreateTranscript(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalCreateTranscriptRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if req.ContentItemID == "" || req.FullText == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "content_item_id and full_text are required"})
		return
	}

	contentUUID, err := uuid.Parse(req.ContentItemID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content_item_id"})
		return
	}

	var item models.ContentItem
	haveItem := db.Where("public_id = ?", contentUUID).First(&item).Error == nil
	var recoveryRequest models.ArtifactCoverageRequest
	var recoveryAttempt models.ArtifactCoverageAttempt
	var stageRequest models.ContentStageRequest
	var stageAttempt models.ContentStageAttempt
	if haveItem {
		if err := requireNormalStageCorrelation(db, item, models.ContentStagePodsTranscript, req.ContentStage, req.ArtifactRecovery != nil); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
	}
	if req.ContentStage != nil {
		if !haveItem {
			c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
			return
		}
		stageRequest, stageAttempt, err = contentstage.AuthorizeWriteback(db, contentUUID, req.ContentStage.correlation(), models.ContentStagePodsTranscript)
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Content-stage transcript correlation is stale"})
			return
		}
	}
	if req.ArtifactRecovery != nil {
		if !haveItem {
			c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
			return
		}
		recoveryRequest, recoveryAttempt, err = artifacts.AuthorizeWriteback(db, contentUUID, artifacts.MediaOwner, artifacts.ArtifactTranscript, req.ArtifactRecovery.correlation())
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Artifact recovery correlation is stale"})
			return
		}
	}
	if req.ArtifactRecovery != nil && req.ContentStage != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Normal stage and recovery correlations are exclusive"})
		return
	}

	source := models.TranscriptSourceSTTDeepgram
	if req.Source != nil && *req.Source != "" {
		source = *req.Source
	}
	var previousTranscriptID *uuid.UUID
	var previousQuality *models.TranscriptQuality
	if haveItem && strings.HasPrefix(source, "stt_") && item.TranscriptID != nil {
		idCopy := *item.TranscriptID
		previousTranscriptID = &idCopy
		var quality models.TranscriptQuality
		if db.Where("tenant_id = ? AND content_item_id = ? AND transcript_id = ?", item.TenantID, item.PublicID, idCopy).First(&quality).Error == nil {
			previousQuality = &quality
		}
	}

	transcript := models.Transcript{
		ContentItemID: contentUUID,
		FullText:      req.FullText,
		Summary:       req.Summary,
		Language:      req.Language,
		Source:        &source,
		Provider:      req.Provider,
	}

	if req.WordTimestamps != nil {
		if raw, err := json.Marshal(req.WordTimestamps); err == nil {
			transcript.WordTimestamps = datatypes.JSON(raw)
		}
	}
	if req.Segments != nil {
		if raw, err := json.Marshal(req.Segments); err == nil {
			transcript.Segments = datatypes.JSON(raw)
		}
	}
	if req.Chapters != nil {
		if raw, err := json.Marshal(req.Chapters); err == nil {
			transcript.Chapters = datatypes.JSON(raw)
		}
	}

	if req.ContentStage != nil {
		captionState := models.CaptionStateForSource(source)
		producerEventID, parseErr := uuid.Parse(req.ContentStage.ProducerEventID)
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content-stage producer event"})
			return
		}
		err = db.Transaction(func(tx *gorm.DB) error {
			// A caller that lost the response reuses the producer event. Return the
			// first durable effect instead of creating a duplicate transcript.
			var existingReceipt models.ContentStageReceipt
			if findErr := tx.Where("tenant_id=? AND owner=? AND producer_event_id=?", item.TenantID, models.ContentStageOwnerMedia, producerEventID).First(&existingReceipt).Error; findErr == nil {
				existingID, idErr := uuid.Parse(existingReceipt.ArtifactDigest)
				if idErr != nil {
					return idErr
				}
				return tx.Where("public_id=? AND content_item_id=?", existingID, contentUUID).First(&transcript).Error
			} else if findErr != gorm.ErrRecordNotFound {
				return findErr
			}
			stageRequest, stageAttempt, authErr := contentstage.AuthorizeWriteback(tx, contentUUID, req.ContentStage.correlation(), models.ContentStagePodsTranscript)
			if authErr != nil {
				return authErr
			}
			if createErr := tx.Create(&transcript).Error; createErr != nil {
				return createErr
			}
			if linkErr := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=?", item.TenantID, contentUUID).Updates(map[string]any{"transcript_id": transcript.PublicID, "caption_state": captionState, "transcript_source": source}).Error; linkErr != nil {
				return linkErr
			}
			return contentstage.RecordPersistence(tx, stageRequest, stageAttempt, req.ContentStage.correlation(), models.ContentStageOwnerMedia, transcript.PublicID.String(), map[string]any{"transcript_id": transcript.PublicID.String(), "provider": req.Provider})
		})
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Durable transcript writeback rejected"})
			return
		}
		item.TranscriptID = &transcript.PublicID
		item.CaptionState = &captionState
		item.TranscriptSource = &source
		computeAndStoreTranscriptQuality(db, &item, &transcript, req.LanguageProbability)
		c.JSON(http.StatusOK, internalCreateTranscriptResponse{ID: transcript.PublicID.String(), CreatedAt: transcript.CreatedAt.UTC().Format(time.RFC3339)})
		return
	}

	if err := db.Create(&transcript).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create transcript"})
		return
	}

	var transcriptionJob *models.TranscriptionJob
	if req.TranscriptionJobID != nil && *req.TranscriptionJobID != "" {
		if id, err := uuid.Parse(*req.TranscriptionJobID); err == nil {
			var job models.TranscriptionJob
			if db.Where("public_id = ?", id).First(&job).Error == nil {
				transcriptionJob = &job
			}
		}
	}
	if transcriptionJob != nil && transcriptionJob.Canceled {
		status := models.TranscriptionJobStatusCanceled
		writebackStatus := "ignored_canceled"
		jobReq := internalUpdateTranscriptionJobRequest{
			Status:          &status,
			TranscriptID:    ptrString(transcript.PublicID.String()),
			Language:        req.Language,
			DurationSec:     req.DurationSec,
			WritebackStatus: &writebackStatus,
			Metadata: map[string]interface{}{
				"write_back_status":      "ignored_canceled",
				"inactive_transcript_id": transcript.PublicID.String(),
			},
		}
		if req.Provider != nil {
			jobReq.Provider = req.Provider
		}
		wasTerminal := terminalTranscriptionStatus(transcriptionJob.Status) && transcriptionJob.CompletedAt != nil
		updateTranscriptionJobFromRequest(db, transcriptionJob, jobReq)
		if err := db.Save(transcriptionJob).Error; err == nil && !wasTerminal && terminalTranscriptionStatus(transcriptionJob.Status) {
			actual := transcriptionJob.ActualCostUsd
			settleTranscriptionBudget(db, transcriptionJob.TenantID, transcriptionJob.ReservedCostUsd, actual)
			updateBatchItemForJob(db, transcriptionJob)
		}
		c.JSON(http.StatusOK, internalCreateTranscriptResponse{
			ID:        transcript.PublicID.String(),
			CreatedAt: transcript.CreatedAt.UTC().Format(time.RFC3339),
		})
		return
	}
	if haveItem && previousTranscriptID != nil {
		snapshotTranscriptVersion(db, item.TenantID, &item, *previousTranscriptID)
	}

	// Link the transcript + set the lightweight caption_state/transcript_source on
	// the content item so feed/console can filter+badge without joining transcripts.
	// Derive source/state with a sane default for legacy callers (no source field).
	captionState := models.CaptionStateForSource(source)

	if haveItem {
		tid := transcript.PublicID
		item.TranscriptID = &tid
		item.CaptionState = &captionState
		item.TranscriptSource = &source
		_ = db.Save(&item).Error
		quality := computeAndStoreTranscriptQuality(db, &item, &transcript, req.LanguageProbability)

		if req.TranscriptionJobID != nil && *req.TranscriptionJobID != "" {
			status := models.TranscriptionJobStatusSucceeded
			writebackStatus := "ok"
			meta := map[string]interface{}{
				"write_back_status": "ok",
				"quality_status":    quality.Status,
				"quality_score":     quality.Score,
			}
			if req.LanguageProbability != nil {
				meta["language_probability"] = *req.LanguageProbability
			}
			jobReq := internalUpdateTranscriptionJobRequest{
				Status:          &status,
				TranscriptID:    ptrString(transcript.PublicID.String()),
				Language:        req.Language,
				DurationSec:     req.DurationSec,
				WritebackStatus: &writebackStatus,
				Metadata:        meta,
			}
			if req.Provider != nil {
				jobReq.Provider = req.Provider
			}
			if transcriptionJob != nil {
				wasTerminal := terminalTranscriptionStatus(transcriptionJob.Status) && transcriptionJob.CompletedAt != nil
				updateTranscriptionJobFromRequest(db, transcriptionJob, jobReq)
				if err := db.Save(transcriptionJob).Error; err == nil {
					if !wasTerminal && terminalTranscriptionStatus(transcriptionJob.Status) {
						actual := transcriptionJob.ActualCostUsd
						if actual == 0 {
							actual = transcriptionJob.EstimatedCostUsd
						}
						settleTranscriptionBudget(db, transcriptionJob.TenantID, transcriptionJob.ReservedCostUsd, actual)
						updateBatchItemForJob(db, transcriptionJob)
						maybeEmitStudioReatomizeAfterCompletion(db, transcriptionJob, &item, previousTranscriptID, previousQuality, quality)
					}
				}
			}
		}

		// Re-enrichment cascade (model-agnostic): when STT upgrades an ALREADY-READY
		// item's transcript, re-trigger embedding so retrieval reflects the better
		// text. The embedding model/composition is owned by the Enrichment migration
		// — we only signal.
		//
		// Gate on READY: this is the manual-upgrade path. During fresh ingestion the
		// item is still PROCESSING and the Aggregation AI worker runs the embedding
		// step itself (with the transcript) right after STT — firing the cascade
		// there would race that write and waste an embedding call.
		if strings.HasPrefix(source, "stt_") && item.Status == models.ContentStatusReady {
			itemCopy := item
			go func() {
				if text := buildEmbeddingText(&itemCopy); text != "" {
					_ = triggerEmbedding(text, itemCopy.PublicID.String())
				}
			}()
		}
		if !strings.HasPrefix(source, "stt_") && quality.Status == models.TranscriptQualityAutoRepair {
			if job, triggered, _, _, err := createTranscriptionJobForItem(db, &item, models.TranscriptionTriggerAutoQuality, false); err == nil && triggered {
				itemCopy := item
				jobID := job.PublicID.String()
				go func() {
					if err := submitTranscriptionJobToMedia(db, &itemCopy, jobID); err != nil {
						_ = updateTranscriptionJobTerminal(db, jobID, models.TranscriptionJobStatusFailed, err.Error())
					}
				}()
			}
		}
	}
	if req.ArtifactRecovery != nil {
		if err := artifacts.RecordPersistence(db, recoveryRequest, recoveryAttempt, req.ArtifactRecovery.correlation(), map[string]any{"transcript_id": transcript.PublicID.String(), "provider": req.Provider}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Transcript persisted but recovery receipt could not be recorded"})
			return
		}
	}
	if req.ContentStage != nil {
		if err := contentstage.RecordPersistence(db, stageRequest, stageAttempt, req.ContentStage.correlation(), models.ContentStageOwnerMedia, transcript.PublicID.String(), map[string]any{"transcript_id": transcript.PublicID.String(), "provider": req.Provider}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Transcript persisted but content-stage receipt could not be recorded"})
			return
		}
	}

	c.JSON(http.StatusOK, internalCreateTranscriptResponse{
		ID:        transcript.PublicID.String(),
		CreatedAt: transcript.CreatedAt.UTC().Format(time.RFC3339),
	})
}

// maybeEmitStudioReatomizeAfterCompletion is deliberately completion-side: an
// accepted STT job is not evidence of an improved transcript. A recommendation
// is emitted only once a Studio-attributed job writes a new transcript that
// resolves the exact prior auto_repair observation. The lead's own idempotency
// gate remains authoritative for duplicate callbacks.
func maybeEmitStudioReatomizeAfterCompletion(db *gorm.DB, job *models.TranscriptionJob, item *models.ContentItem, previousTranscriptID *uuid.UUID, previousQuality *models.TranscriptQuality, quality models.TranscriptQuality) {
	if job == nil || item == nil || previousTranscriptID == nil || previousQuality == nil {
		return
	}
	if job.TriggerSource != models.TranscriptionTriggerStudioAutopilot || job.Status != models.TranscriptionJobStatusSucceeded {
		return
	}
	if previousQuality.Status != models.TranscriptQualityAutoRepair || quality.Status != models.TranscriptQualityOK || quality.TranscriptID == *previousTranscriptID {
		return
	}
	recommendation := emitReatomizeRecommendation(db, item.TenantID, item)
	if recommendation == nil {
		return
	}
	payload, _ := json.Marshal(map[string]string{
		"transcription_job_id": job.PublicID.String(), "previous_transcript_id": previousTranscriptID.String(),
		"transcript_id": quality.TranscriptID.String(), "recommendation_id": recommendation.PublicID.String(),
	})
	_ = db.Create(&models.AuditLog{TenantID: item.TenantID, UserEmail: models.StudioAuditPrincipal, Action: "media_studio.transcript_reatomize_recommended", TargetService: "cms", TargetResource: item.PublicID.String(), Status: "success", Payload: datatypes.JSON(payload)}).Error
}

func ptrString(s string) *string {
	return &s
}
