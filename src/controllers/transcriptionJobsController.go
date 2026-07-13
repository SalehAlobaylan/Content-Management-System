package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var errTranscriptionBudgetCapReached = errors.New("monthly STT budget cap reached")

type transcriptionJobResponse struct {
	ID                string                 `json:"id"`
	ContentItemID     string                 `json:"content_item_id"`
	ContentTitle      string                 `json:"content_title,omitempty"`
	ContentType       string                 `json:"content_type,omitempty"`
	ContentSourceName string                 `json:"content_source_name,omitempty"`
	FileSizeBytes     int64                  `json:"file_size_bytes,omitempty"`
	StorageTier       string                 `json:"storage_tier,omitempty"`
	TranscriptID      *string                `json:"transcript_id,omitempty"`
	BatchID           *string                `json:"batch_id,omitempty"`
	BatchItemID       *string                `json:"batch_item_id,omitempty"`
	TriggerSource     string                 `json:"trigger_source"`
	Status            string                 `json:"status"`
	Provider          string                 `json:"provider,omitempty"`
	Model             string                 `json:"model,omitempty"`
	Language          string                 `json:"language,omitempty"`
	SkipReason        string                 `json:"skip_reason,omitempty"`
	ErrorMessage      string                 `json:"error_message,omitempty"`
	ProviderErrorCode string                 `json:"provider_error_code,omitempty"`
	RetryCount        int                    `json:"retry_count"`
	Canceled          bool                   `json:"canceled"`
	EstimatedCostUsd  float64                `json:"estimated_cost_usd"`
	ReservedCostUsd   float64                `json:"reserved_cost_usd"`
	ActualCostUsd     float64                `json:"actual_cost_usd"`
	MediaJobID        string                 `json:"media_job_id,omitempty"`
	WritebackStatus   string                 `json:"writeback_status,omitempty"`
	WritebackError    string                 `json:"writeback_error,omitempty"`
	StartedAt         *string                `json:"started_at,omitempty"`
	CompletedAt       *string                `json:"completed_at,omitempty"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt         string                 `json:"created_at"`
	UpdatedAt         string                 `json:"updated_at"`
}

type transcriptQualityResponse struct {
	ID            string                 `json:"id"`
	ContentItemID string                 `json:"content_item_id"`
	TranscriptID  string                 `json:"transcript_id"`
	Score         float64                `json:"score"`
	Status        string                 `json:"status"`
	IssueCodes    []string               `json:"issue_codes"`
	Details       map[string]interface{} `json:"details,omitempty"`
	ComputedAt    string                 `json:"computed_at"`
}

func terminalTranscriptionStatus(status string) bool {
	switch status {
	case models.TranscriptionJobStatusSkipped,
		models.TranscriptionJobStatusSucceeded,
		models.TranscriptionJobStatusFailed,
		models.TranscriptionJobStatusWritebackFailed,
		models.TranscriptionJobStatusCanceled:
		return true
	default:
		return false
	}
}

func formatTimePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

func mapTranscriptionJob(job models.TranscriptionJob) transcriptionJobResponse {
	var meta map[string]interface{}
	if len(job.Metadata) > 0 {
		_ = json.Unmarshal(job.Metadata, &meta)
	}
	var transcriptID *string
	if job.TranscriptID != nil {
		s := job.TranscriptID.String()
		transcriptID = &s
	}
	var batchID *string
	if job.BatchID != nil {
		s := job.BatchID.String()
		batchID = &s
	}
	var batchItemID *string
	if job.BatchItemID != nil {
		s := job.BatchItemID.String()
		batchItemID = &s
	}
	return transcriptionJobResponse{
		ID:                job.PublicID.String(),
		ContentItemID:     job.ContentItemID.String(),
		TranscriptID:      transcriptID,
		BatchID:           batchID,
		BatchItemID:       batchItemID,
		TriggerSource:     job.TriggerSource,
		Status:            job.Status,
		Provider:          job.Provider,
		Model:             job.Model,
		Language:          job.Language,
		SkipReason:        job.SkipReason,
		ErrorMessage:      job.ErrorMessage,
		ProviderErrorCode: job.ProviderErrorCode,
		RetryCount:        job.RetryCount,
		Canceled:          job.Canceled,
		EstimatedCostUsd:  job.EstimatedCostUsd,
		ReservedCostUsd:   job.ReservedCostUsd,
		ActualCostUsd:     job.ActualCostUsd,
		MediaJobID:        job.MediaJobID,
		WritebackStatus:   job.WritebackStatus,
		WritebackError:    job.WritebackError,
		StartedAt:         formatTimePtr(job.StartedAt),
		CompletedAt:       formatTimePtr(job.CompletedAt),
		Metadata:          meta,
		CreatedAt:         job.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:         job.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func mapTranscriptionJobWithContent(job models.TranscriptionJob, item *models.ContentItem) transcriptionJobResponse {
	out := mapTranscriptionJob(job)
	if item == nil {
		return out
	}
	if item.Title != nil {
		out.ContentTitle = *item.Title
	}
	out.ContentType = string(item.Type)
	if item.SourceName != nil {
		out.ContentSourceName = *item.SourceName
	}
	out.FileSizeBytes = item.FileSizeBytes
	if item.StorageTier != nil {
		out.StorageTier = *item.StorageTier
	}
	return out
}

func mapTranscriptQuality(q models.TranscriptQuality) transcriptQualityResponse {
	var details map[string]interface{}
	if len(q.Details) > 0 {
		_ = json.Unmarshal(q.Details, &details)
	}
	return transcriptQualityResponse{
		ID:            q.PublicID.String(),
		ContentItemID: q.ContentItemID.String(),
		TranscriptID:  q.TranscriptID.String(),
		Score:         q.Score,
		Status:        q.Status,
		IssueCodes:    []string(q.IssueCodes),
		Details:       details,
		ComputedAt:    q.ComputedAt.UTC().Format(time.RFC3339),
	}
}

func estimateSTTCostForDuration(durationSec float64) float64 {
	if durationSec <= 0 {
		return 0
	}
	return (durationSec / 3600.0) * sttEstimatedCostPerHourUsd
}

func settleTranscriptionBudget(db *gorm.DB, tenantID string, reserved, actual float64) {
	if reserved <= 0 && actual <= 0 {
		return
	}
	updates := map[string]interface{}{}
	if reserved > 0 {
		updates["monthly_reserved_usd"] = gorm.Expr("GREATEST(monthly_reserved_usd - ?, 0)", reserved)
	}
	if actual > 0 {
		updates["monthly_spend_usd"] = gorm.Expr("monthly_spend_usd + ?", actual)
	}
	db.Model(&models.TranscriptionConfig{}).Where("tenant_id = ?", tenantID).Updates(updates)
}

func createSkippedTranscriptionJob(db *gorm.DB, item *models.ContentItem, triggerSource, reason string) models.TranscriptionJob {
	now := time.Now()
	job := models.TranscriptionJob{
		TenantID:         item.TenantID,
		ContentItemID:    item.PublicID,
		TranscriptID:     item.TranscriptID,
		TriggerSource:    triggerSource,
		Status:           models.TranscriptionJobStatusSkipped,
		SkipReason:       reason,
		EstimatedCostUsd: estimateSTTCostUSD(item.DurationSec),
		CompletedAt:      &now,
	}
	_ = db.Create(&job).Error
	return job
}

func createAcceptedTranscriptionJob(db *gorm.DB, item *models.ContentItem, triggerSource string) (models.TranscriptionJob, error) {
	est := estimateSTTCostUSD(item.DurationSec)
	var job models.TranscriptionJob
	err := db.Transaction(func(tx *gorm.DB) error {
		var cfg models.TranscriptionConfig
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id = ?", item.TenantID).
			First(&cfg).Error; err != nil {
			return err
		}
		if cfg.MonthlyBudgetCapUsd > 0 && cfg.MonthlySpendUsd+cfg.MonthlyReservedUsd+est > cfg.MonthlyBudgetCapUsd {
			return errTranscriptionBudgetCapReached
		}
		job = models.TranscriptionJob{
			TenantID:         item.TenantID,
			ContentItemID:    item.PublicID,
			TranscriptID:     item.TranscriptID,
			TriggerSource:    triggerSource,
			Status:           models.TranscriptionJobStatusQueued,
			EstimatedCostUsd: est,
			ReservedCostUsd:  est,
		}
		if err := tx.Create(&job).Error; err != nil {
			return err
		}
		if est > 0 {
			if err := tx.Model(&cfg).UpdateColumn("monthly_reserved_usd", gorm.Expr("monthly_reserved_usd + ?", est)).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return job, err
	}
	return job, nil
}

// sttSkipKind classifies guard skips so consumers never need to parse the
// human-readable reason that is shown in the ledger and Console.
type sttSkipKind string

const (
	sttSkipNone   sttSkipKind = ""
	sttSkipBudget sttSkipKind = "budget"
	sttSkipGuard  sttSkipKind = "guard"
)

func latestTranscriptQuality(db *gorm.DB, contentID uuid.UUID) *models.TranscriptQuality {
	var q models.TranscriptQuality
	if err := db.Where("content_item_id = ?", contentID).First(&q).Error; err != nil {
		return nil
	}
	return &q
}

// evaluateSTTAdmission answers whether an unforced trigger would be accepted
// without creating a job. Observe mode uses this same decision as the live path.
func evaluateSTTAdmission(db *gorm.DB, item *models.ContentItem, triggerSource string) (bool, string) {
	cfg := getOrCreateTranscriptionConfig(db, item.TenantID)
	state := ""
	if item.CaptionState != nil {
		state = *item.CaptionState
	}
	if triggerSource == "" {
		triggerSource = models.TranscriptionTriggerManual
	}
	if item.MediaURL == nil || strings.TrimSpace(*item.MediaURL) == "" {
		return false, "no media_url available"
	}
	if state == models.CaptionStateYouTubeHuman {
		return false, "human caption present (no STT needed)"
	}
	qualityDriven := triggerSource == models.TranscriptionTriggerAutoQuality || triggerSource == models.TranscriptionTriggerStudioAutopilot
	if state == models.CaptionStateSTTDone {
		q := latestTranscriptQuality(db, item.PublicID)
		if q == nil || q.Status != models.TranscriptQualityAutoRepair || !cfg.AutoRepairEnabled {
			return false, "already upgraded by STT"
		}
	}
	if state == models.CaptionStateYouTubeAuto && !cfg.AutoSttEnabled && !qualityDriven {
		return false, "auto-STT disabled (manual trigger required)"
	}
	if (state == "" || state == models.CaptionStateNone) && !cfg.AutoSttEnabled && triggerSource != models.TranscriptionTriggerManual && triggerSource != models.TranscriptionTriggerBulkManual && triggerSource != models.TranscriptionTriggerAutoQuality {
		return false, "auto-STT disabled (manual trigger required)"
	}
	if estimate := estimateSTTCostUSD(item.DurationSec); cfg.MonthlyBudgetCapUsd > 0 && cfg.MonthlySpendUsd+cfg.MonthlyReservedUsd+estimate > cfg.MonthlyBudgetCapUsd {
		return false, "monthly STT budget cap reached"
	}
	return true, ""
}

func createTranscriptionJobForItem(db *gorm.DB, item *models.ContentItem, triggerSource string, force bool) (models.TranscriptionJob, bool, string, sttSkipKind, error) {
	if triggerSource == "" {
		triggerSource = models.TranscriptionTriggerManual
	}
	if item.MediaURL == nil || strings.TrimSpace(*item.MediaURL) == "" {
		job := createSkippedTranscriptionJob(db, item, triggerSource, "no media_url available")
		return job, false, job.SkipReason, sttSkipGuard, nil
	}
	if !force {
		admit, reason := evaluateSTTAdmission(db, item, triggerSource)
		if !admit {
			job := createSkippedTranscriptionJob(db, item, triggerSource, reason)
			kind := sttSkipGuard
			if reason == "monthly STT budget cap reached" {
				kind = sttSkipBudget
			}
			return job, false, job.SkipReason, kind, nil
		}
		if item.CaptionState != nil && *item.CaptionState == models.CaptionStateSTTDone && triggerSource != models.TranscriptionTriggerAutoQuality && triggerSource != models.TranscriptionTriggerStudioAutopilot {
			triggerSource = models.TranscriptionTriggerAutoQuality
		}
	}

	job, err := createAcceptedTranscriptionJob(db, item, triggerSource)
	if err != nil {
		if errors.Is(err, errTranscriptionBudgetCapReached) {
			job := createSkippedTranscriptionJob(db, item, triggerSource, "monthly STT budget cap reached")
			return job, false, job.SkipReason, sttSkipBudget, nil
		}
		return job, false, "", sttSkipNone, err
	}
	return job, true, "", sttSkipNone, nil
}

func checksumTranscriptText(text string, segments datatypes.JSON) string {
	h := sha256.New()
	h.Write([]byte(text))
	h.Write([]byte{0})
	h.Write([]byte(segments))
	return hex.EncodeToString(h.Sum(nil))
}

func snapshotTranscriptVersion(db *gorm.DB, tenantID string, item *models.ContentItem, transcriptID uuid.UUID) {
	var existing models.Transcript
	if err := db.Where("public_id = ?", transcriptID).First(&existing).Error; err != nil {
		return
	}
	row := models.TranscriptVersion{
		TenantID:              tenantID,
		ContentItemID:         item.PublicID,
		TranscriptID:          existing.PublicID,
		FullText:              existing.FullText,
		Summary:               existing.Summary,
		WordTimestamps:        existing.WordTimestamps,
		Segments:              existing.Segments,
		Chapters:              existing.Chapters,
		Language:              existing.Language,
		Source:                existing.Source,
		Provider:              existing.Provider,
		ApprovedAt:            existing.ApprovedAt,
		ApprovedBy:            existing.ApprovedBy,
		ApprovalReason:        existing.ApprovalReason,
		Checksum:              checksumTranscriptText(existing.FullText, existing.Segments),
		Reason:                "stt_replacement",
		EmbeddingsRegenerated: true,
	}
	_ = db.Create(&row).Error
}

func computeAndStoreTranscriptQuality(db *gorm.DB, item *models.ContentItem, transcript *models.Transcript, confidence *float64) models.TranscriptQuality {
	cfg := getOrCreateTranscriptionConfig(db, item.TenantID)
	issues := []string{}
	details := map[string]interface{}{}
	score := 1.0

	segments := extractSegments(transcript)
	wordCount := len(strings.Fields(transcript.FullText))
	durationSec := 0
	if item.DurationSec != nil {
		durationSec = *item.DurationSec
	}
	details["word_count"] = wordCount
	details["segment_count"] = len(segments)
	details["duration_sec"] = durationSec

	if wordCount == 0 {
		issues = append(issues, "empty_text")
		score -= 0.7
	}
	if durationSec >= 60 && wordCount < durationSec/12 {
		issues = append(issues, "low_word_count_for_duration")
		score -= 0.25
	}
	if len(segments) == 0 {
		issues = append(issues, "missing_segments")
		score -= 0.2
	} else {
		empty := 0
		repeated := 0
		prev := ""
		for _, s := range segments {
			txt := strings.TrimSpace(s.Text)
			if txt == "" {
				empty++
			}
			if txt != "" && txt == prev {
				repeated++
			}
			if txt != "" {
				prev = txt
			}
		}
		emptyRatio := float64(empty) / float64(len(segments))
		repeatRatio := float64(repeated) / float64(len(segments))
		details["empty_segment_ratio"] = emptyRatio
		details["repeated_segment_ratio"] = repeatRatio
		if emptyRatio > 0.2 {
			issues = append(issues, "many_empty_segments")
			score -= 0.2
		}
		if repeatRatio > 0.2 {
			issues = append(issues, "repeated_segments")
			score -= 0.2
		}
	}

	lang := ""
	if transcript.Language != nil {
		lang = strings.ToLower(*transcript.Language)
	}
	if strings.HasPrefix(lang, "ar") {
		letters := 0
		arabic := 0
		for _, r := range transcript.FullText {
			if unicode.IsLetter(r) {
				letters++
				if unicode.In(r, unicode.Arabic) {
					arabic++
				}
			}
		}
		ratio := 0.0
		if letters > 0 {
			ratio = float64(arabic) / float64(letters)
		}
		details["arabic_script_ratio"] = ratio
		if letters > 20 && ratio < 0.35 {
			issues = append(issues, "arabic_script_low")
			score -= 0.35
		}
	}
	if confidence != nil {
		details["language_probability"] = *confidence
		if *confidence > 0 && *confidence < 0.55 {
			issues = append(issues, "low_provider_confidence")
			score -= 0.2
		}
	}

	score = math.Max(0, math.Min(1, score))
	status := models.TranscriptQualityOK
	if score < cfg.QualityAutoRepairThreshold {
		status = models.TranscriptQualityAutoRepair
	} else if score < cfg.QualityReviewThreshold || len(issues) > 0 {
		status = models.TranscriptQualityNeedsReview
	}
	rawDetails, _ := json.Marshal(details)
	now := time.Now()
	q := models.TranscriptQuality{
		TenantID:      item.TenantID,
		ContentItemID: item.PublicID,
		TranscriptID:  transcript.PublicID,
		Score:         score,
		Status:        status,
		IssueCodes:    pq.StringArray(issues),
		Details:       datatypes.JSON(rawDetails),
		ComputedAt:    now,
	}
	var existing models.TranscriptQuality
	if err := db.Where("content_item_id = ?", item.PublicID).First(&existing).Error; err == nil {
		existing.TranscriptID = q.TranscriptID
		existing.Score = q.Score
		existing.Status = q.Status
		existing.IssueCodes = q.IssueCodes
		existing.Details = q.Details
		existing.ComputedAt = q.ComputedAt
		_ = db.Save(&existing).Error
		return existing
	}
	_ = db.Create(&q).Error
	return q
}

func latestTranscriptionJob(db *gorm.DB, contentID uuid.UUID) *models.TranscriptionJob {
	var job models.TranscriptionJob
	if err := db.Where("content_item_id = ?", contentID).Order("created_at DESC").First(&job).Error; err != nil {
		return nil
	}
	return &job
}

func submitTranscriptionJobToMedia(db *gorm.DB, item *models.ContentItem, jobID string) error {
	mediaJobID, err := triggerTranscriptionForJob(item, jobID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(mediaJobID) != "" {
		db.Model(&models.TranscriptionJob{}).
			Where("public_id = ?", jobID).
			Update("media_job_id", mediaJobID)
	}
	return nil
}

func updateTranscriptionJobFromRequest(db *gorm.DB, job *models.TranscriptionJob, req internalUpdateTranscriptionJobRequest) {
	now := time.Now()
	if req.Status != nil {
		job.Status = strings.TrimSpace(*req.Status)
		if job.Status == models.TranscriptionJobStatusRunning && job.StartedAt == nil {
			job.StartedAt = &now
		}
		if terminalTranscriptionStatus(job.Status) && job.CompletedAt == nil {
			job.CompletedAt = &now
		}
	}
	if req.Provider != nil {
		job.Provider = *req.Provider
	}
	if req.Model != nil {
		job.Model = *req.Model
	}
	if req.Language != nil {
		job.Language = *req.Language
	}
	if req.SkipReason != nil {
		job.SkipReason = *req.SkipReason
	}
	if req.ErrorMessage != nil {
		job.ErrorMessage = *req.ErrorMessage
	}
	if req.ProviderErrorCode != nil {
		job.ProviderErrorCode = *req.ProviderErrorCode
	}
	if req.RetryCount != nil {
		job.RetryCount = *req.RetryCount
	}
	if req.Canceled != nil {
		job.Canceled = *req.Canceled
	}
	if req.TranscriptID != nil {
		if id, err := uuid.Parse(*req.TranscriptID); err == nil {
			job.TranscriptID = &id
		}
	}
	if req.MediaJobID != nil {
		job.MediaJobID = *req.MediaJobID
	}
	if req.WritebackStatus != nil {
		job.WritebackStatus = *req.WritebackStatus
	}
	if req.WritebackError != nil {
		job.WritebackError = *req.WritebackError
	}
	if req.DurationSec != nil && *req.DurationSec > 0 {
		job.ActualCostUsd = estimateSTTCostForDuration(*req.DurationSec)
	}
	if req.ActualCostUsd != nil {
		job.ActualCostUsd = *req.ActualCostUsd
	}
	if req.Metadata != nil {
		meta := map[string]interface{}{}
		if len(job.Metadata) > 0 {
			_ = json.Unmarshal(job.Metadata, &meta)
		}
		for k, v := range req.Metadata {
			meta[k] = v
		}
		raw, _ := json.Marshal(meta)
		job.Metadata = datatypes.JSON(raw)
	}
}

type internalUpdateTranscriptionJobRequest struct {
	Status            *string                `json:"status"`
	Provider          *string                `json:"provider"`
	Model             *string                `json:"model"`
	Language          *string                `json:"language"`
	SkipReason        *string                `json:"skip_reason"`
	ErrorMessage      *string                `json:"error_message"`
	ProviderErrorCode *string                `json:"provider_error_code"`
	RetryCount        *int                   `json:"retry_count"`
	Canceled          *bool                  `json:"canceled"`
	TranscriptID      *string                `json:"transcript_id"`
	MediaJobID        *string                `json:"media_job_id"`
	WritebackStatus   *string                `json:"writeback_status"`
	WritebackError    *string                `json:"writeback_error"`
	DurationSec       *float64               `json:"duration_sec"`
	ActualCostUsd     *float64               `json:"actual_cost_usd"`
	Metadata          map[string]interface{} `json:"metadata"`
}

func InternalUpdateTranscriptionJob(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid transcription job ID"})
		return
	}
	var job models.TranscriptionJob
	if err := db.Where("public_id = ?", id).First(&job).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Transcription job not found"})
		return
	}
	var req internalUpdateTranscriptionJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	wasTerminal := terminalTranscriptionStatus(job.Status) && job.CompletedAt != nil
	updateTranscriptionJobFromRequest(db, &job, req)
	if err := db.Save(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update transcription job"})
		return
	}
	if !wasTerminal && terminalTranscriptionStatus(job.Status) {
		actual := 0.0
		if job.Status == models.TranscriptionJobStatusSucceeded || job.Status == models.TranscriptionJobStatusWritebackFailed ||
			(job.Status == models.TranscriptionJobStatusCanceled && job.ActualCostUsd > 0) {
			actual = job.ActualCostUsd
			if actual == 0 {
				actual = job.EstimatedCostUsd
			}
		}
		settleTranscriptionBudget(db, job.TenantID, job.ReservedCostUsd, actual)
		updateBatchItemForJob(db, &job)
	}
	c.JSON(http.StatusOK, mapTranscriptionJob(job))
}

func InternalCompleteTranscriptionJob(c *gin.Context) {
	InternalUpdateTranscriptionJob(c)
}

type createTranscriptionJobRequest struct {
	ContentID     string `json:"content_id"`
	Force         bool   `json:"force"`
	TriggerSource string `json:"trigger_source"`
}

func CreateTranscriptionJob(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req createTranscriptionJobRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.ContentID) == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "content_id is required", Code: "INVALID_REQUEST"})
		return
	}
	contentID, err := uuid.Parse(req.ContentID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid content ID", Code: "INVALID_ID"})
		return
	}
	var item models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", contentID, principal.TenantID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Content not found", Code: "NOT_FOUND"})
		return
	}
	trigger := req.TriggerSource
	if trigger == "" {
		trigger = models.TranscriptionTriggerManual
	}
	job, triggered, reason, _, err := createTranscriptionJobForItem(db, &item, trigger, req.Force)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create transcription job", Code: "CREATE_FAILED"})
		return
	}
	if triggered {
		itemCopy := item
		jobID := job.PublicID.String()
		go func() {
			if err := submitTranscriptionJobToMedia(db, &itemCopy, jobID); err != nil {
				msg := err.Error()
				status := models.TranscriptionJobStatusFailed
				_ = updateTranscriptionJobTerminal(db, jobID, status, msg)
			}
		}()
	}
	c.JSON(http.StatusAccepted, utils.ResponseMessage{
		Code:    http.StatusAccepted,
		Message: "Transcription job accepted",
		Data: gin.H{
			"job":       mapTranscriptionJob(job),
			"triggered": triggered,
			"reason":    reason,
		},
	})
}

type bulkTranscriptionJobRequest struct {
	ContentIDs []string `json:"content_ids"`
	Force      bool     `json:"force"`
}

type transcriptionBatchItemResponse struct {
	ID            string                    `json:"id"`
	ContentItemID string                    `json:"content_item_id"`
	JobID         *string                   `json:"job_id,omitempty"`
	Job           *transcriptionJobResponse `json:"job,omitempty"`
	Status        string                    `json:"status"`
	Reason        string                    `json:"reason,omitempty"`
	Error         string                    `json:"error,omitempty"`
	CreatedAt     string                    `json:"created_at"`
	UpdatedAt     string                    `json:"updated_at"`
}

type transcriptionBatchResponse struct {
	ID             string                           `json:"id"`
	Status         string                           `json:"status"`
	Force          bool                             `json:"force"`
	Actor          string                           `json:"actor,omitempty"`
	TotalCount     int                              `json:"total_count"`
	AcceptedCount  int                              `json:"accepted_count"`
	SkippedCount   int                              `json:"skipped_count"`
	FailedCount    int                              `json:"failed_count"`
	CanceledCount  int                              `json:"canceled_count"`
	CompletedCount int                              `json:"completed_count"`
	LatestError    string                           `json:"latest_error,omitempty"`
	CanceledAt     *string                          `json:"canceled_at,omitempty"`
	CompletedAt    *string                          `json:"completed_at,omitempty"`
	CreatedAt      string                           `json:"created_at"`
	UpdatedAt      string                           `json:"updated_at"`
	Items          []transcriptionBatchItemResponse `json:"items,omitempty"`
}

type transcriptionBatchListResponse struct {
	Data       []transcriptionBatchResponse `json:"data"`
	Total      int64                        `json:"total"`
	Page       int                          `json:"page"`
	Limit      int                          `json:"limit"`
	TotalPages int                          `json:"total_pages"`
}

type createTranscriptionBatchRequest struct {
	ContentIDs []string `json:"content_ids"`
	Force      *bool    `json:"force"`
}

func mapTranscriptionBatch(batch models.TranscriptionBatch, items []models.TranscriptionBatchItem) transcriptionBatchResponse {
	return mapTranscriptionBatchWithJobs(batch, items, nil)
}

func mapTranscriptionBatchWithJobs(batch models.TranscriptionBatch, items []models.TranscriptionBatchItem, jobs map[uuid.UUID]transcriptionJobResponse) transcriptionBatchResponse {
	out := transcriptionBatchResponse{
		ID:             batch.PublicID.String(),
		Status:         batch.Status,
		Force:          batch.Force,
		Actor:          batch.Actor,
		TotalCount:     batch.TotalCount,
		AcceptedCount:  batch.AcceptedCount,
		SkippedCount:   batch.SkippedCount,
		FailedCount:    batch.FailedCount,
		CanceledCount:  batch.CanceledCount,
		CompletedCount: batch.CompletedCount,
		LatestError:    batch.LatestError,
		CanceledAt:     formatTimePtr(batch.CanceledAt),
		CompletedAt:    formatTimePtr(batch.CompletedAt),
		CreatedAt:      batch.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      batch.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if items != nil {
		out.Items = make([]transcriptionBatchItemResponse, 0, len(items))
		for _, item := range items {
			var jobID *string
			if item.JobID != nil {
				s := item.JobID.String()
				jobID = &s
			}
			var job *transcriptionJobResponse
			if item.JobID != nil && jobs != nil {
				if mapped, ok := jobs[*item.JobID]; ok {
					mappedCopy := mapped
					job = &mappedCopy
				}
			}
			out.Items = append(out.Items, transcriptionBatchItemResponse{
				ID:            item.PublicID.String(),
				ContentItemID: item.ContentItemID.String(),
				JobID:         jobID,
				Job:           job,
				Status:        item.Status,
				Reason:        item.Reason,
				Error:         item.Error,
				CreatedAt:     item.CreatedAt.UTC().Format(time.RFC3339),
				UpdatedAt:     item.UpdatedAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return out
}

func loadTranscriptionJobSummariesForBatchItems(db *gorm.DB, tenantID string, items []models.TranscriptionBatchItem) map[uuid.UUID]transcriptionJobResponse {
	jobIDs := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		if item.JobID != nil {
			jobIDs = append(jobIDs, *item.JobID)
		}
	}
	if len(jobIDs) == 0 {
		return nil
	}
	var jobs []models.TranscriptionJob
	db.Where("tenant_id = ? AND public_id IN ?", tenantID, jobIDs).Find(&jobs)
	contentIDs := make([]uuid.UUID, 0, len(jobs))
	for _, job := range jobs {
		contentIDs = append(contentIDs, job.ContentItemID)
	}
	var contentItems []models.ContentItem
	if len(contentIDs) > 0 {
		db.Where("tenant_id = ? AND public_id IN ?", tenantID, contentIDs).Find(&contentItems)
	}
	contentByID := map[uuid.UUID]models.ContentItem{}
	for _, item := range contentItems {
		contentByID[item.PublicID] = item
	}
	out := map[uuid.UUID]transcriptionJobResponse{}
	for _, job := range jobs {
		if item, ok := contentByID[job.ContentItemID]; ok {
			out[job.PublicID] = mapTranscriptionJobWithContent(job, &item)
		} else {
			out[job.PublicID] = mapTranscriptionJob(job)
		}
	}
	return out
}

func recomputeTranscriptionBatch(db *gorm.DB, batchID uuid.UUID) {
	var items []models.TranscriptionBatchItem
	db.Where("batch_id = ?", batchID).Find(&items)
	counts := map[string]int{}
	latestError := ""
	accepted := 0
	for _, item := range items {
		counts[item.Status]++
		if item.Status == models.TranscriptionBatchItemStatusAccepted ||
			item.Status == models.TranscriptionBatchItemStatusDone ||
			(item.Status == models.TranscriptionBatchItemStatusFailed && item.JobID != nil) ||
			(item.Status == models.TranscriptionBatchItemStatusCanceled && item.JobID != nil) {
			accepted++
		}
		if item.Error != "" {
			latestError = item.Error
		}
	}
	var batch models.TranscriptionBatch
	if err := db.Where("public_id = ?", batchID).First(&batch).Error; err != nil {
		return
	}
	batch.AcceptedCount = accepted
	batch.SkippedCount = counts[models.TranscriptionBatchItemStatusSkipped]
	batch.FailedCount = counts[models.TranscriptionBatchItemStatusFailed]
	batch.CanceledCount = counts[models.TranscriptionBatchItemStatusCanceled]
	batch.CompletedCount = counts[models.TranscriptionBatchItemStatusDone]
	if latestError != "" {
		batch.LatestError = latestError
	}
	terminal := batch.SkippedCount + batch.FailedCount + batch.CanceledCount + batch.CompletedCount
	if terminal >= batch.TotalCount {
		now := time.Now()
		if batch.CompletedAt == nil {
			batch.CompletedAt = &now
		}
		if batch.CanceledAt != nil {
			batch.Status = models.TranscriptionBatchStatusCanceled
		} else if batch.FailedCount > 0 {
			batch.Status = models.TranscriptionBatchStatusFailed
		} else {
			batch.Status = models.TranscriptionBatchStatusCompleted
		}
	} else if batch.CanceledAt != nil {
		batch.Status = models.TranscriptionBatchStatusCanceled
	} else {
		batch.Status = models.TranscriptionBatchStatusRunning
	}
	_ = db.Save(&batch).Error
}

func updateBatchItemForJob(db *gorm.DB, job *models.TranscriptionJob) {
	if job.BatchID == nil || job.BatchItemID == nil {
		return
	}
	status := ""
	reason := ""
	errMsg := ""
	switch job.Status {
	case models.TranscriptionJobStatusSucceeded:
		if job.Canceled {
			status = models.TranscriptionBatchItemStatusCanceled
			reason = "batch canceled before writeback activation"
		} else {
			status = models.TranscriptionBatchItemStatusDone
		}
	case models.TranscriptionJobStatusSkipped:
		status = models.TranscriptionBatchItemStatusSkipped
		reason = job.SkipReason
	case models.TranscriptionJobStatusCanceled:
		status = models.TranscriptionBatchItemStatusCanceled
		reason = job.SkipReason
	case models.TranscriptionJobStatusFailed, models.TranscriptionJobStatusWritebackFailed:
		if job.Canceled {
			status = models.TranscriptionBatchItemStatusCanceled
			reason = "batch canceled"
		} else {
			status = models.TranscriptionBatchItemStatusFailed
			errMsg = job.ErrorMessage
			if errMsg == "" {
				errMsg = job.WritebackError
			}
		}
	default:
		return
	}
	db.Model(&models.TranscriptionBatchItem{}).
		Where("public_id = ?", *job.BatchItemID).
		Updates(map[string]interface{}{"status": status, "reason": reason, "error": errMsg})
	recomputeTranscriptionBatch(db, *job.BatchID)
}

func dispatchTranscriptionBatch(db *gorm.DB, batchID uuid.UUID) {
	db.Model(&models.TranscriptionBatch{}).
		Where("public_id = ? AND status = ?", batchID, models.TranscriptionBatchStatusQueued).
		Update("status", models.TranscriptionBatchStatusRunning)

	var batch models.TranscriptionBatch
	if err := db.Where("public_id = ?", batchID).First(&batch).Error; err != nil {
		return
	}
	var items []models.TranscriptionBatchItem
	db.Where("batch_id = ? AND status IN ?", batchID, []string{
		models.TranscriptionBatchItemStatusPending,
		models.TranscriptionBatchItemStatusAccepted,
	}).
		Order("created_at ASC").Find(&items)
	for _, batchItem := range items {
		var submitItem *models.ContentItem
		var submitJobID string
		err := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("public_id = ?", batchID).
				First(&batch).Error; err != nil {
				return err
			}
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
				Where("public_id = ? AND status IN ?", batchItem.PublicID, []string{
					models.TranscriptionBatchItemStatusPending,
					models.TranscriptionBatchItemStatusAccepted,
				}).
				First(&batchItem).Error; err != nil {
				return err
			}
			if batch.CanceledAt != nil {
				return tx.Model(&batchItem).Updates(map[string]interface{}{"status": models.TranscriptionBatchItemStatusCanceled, "reason": "batch canceled"}).Error
			}
			if batchItem.Status == models.TranscriptionBatchItemStatusAccepted && batchItem.JobID != nil {
				var existingJob models.TranscriptionJob
				if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ?", *batchItem.JobID).First(&existingJob).Error; err != nil {
					return err
				}
				if terminalTranscriptionStatus(existingJob.Status) {
					status := models.TranscriptionBatchItemStatusFailed
					reason := ""
					errMsg := existingJob.ErrorMessage
					switch existingJob.Status {
					case models.TranscriptionJobStatusSucceeded:
						status = models.TranscriptionBatchItemStatusDone
					case models.TranscriptionJobStatusSkipped:
						status = models.TranscriptionBatchItemStatusSkipped
						reason = existingJob.SkipReason
					case models.TranscriptionJobStatusCanceled:
						status = models.TranscriptionBatchItemStatusCanceled
						reason = existingJob.SkipReason
					case models.TranscriptionJobStatusWritebackFailed:
						if errMsg == "" {
							errMsg = existingJob.WritebackError
						}
					}
					return tx.Model(&batchItem).Updates(map[string]interface{}{"status": status, "reason": reason, "error": errMsg}).Error
				}
				if existingJob.MediaJobID != "" {
					return nil
				}
				if existingJob.Status == models.TranscriptionJobStatusRunning &&
					existingJob.StartedAt != nil &&
					time.Since(*existingJob.StartedAt) < 2*time.Minute {
					return nil
				}
				var item models.ContentItem
				if err := tx.Where("public_id = ? AND tenant_id = ?", batchItem.ContentItemID, batch.TenantID).First(&item).Error; err != nil {
					existingJob.Status = models.TranscriptionJobStatusFailed
					existingJob.ErrorMessage = "content item not found"
					now := time.Now()
					existingJob.CompletedAt = &now
					if saveErr := tx.Save(&existingJob).Error; saveErr != nil {
						return saveErr
					}
					settleTranscriptionBudget(tx, existingJob.TenantID, existingJob.ReservedCostUsd, 0)
					return tx.Model(&batchItem).Updates(map[string]interface{}{"status": models.TranscriptionBatchItemStatusFailed, "error": "content item not found"}).Error
				}
				existingJob.Status = models.TranscriptionJobStatusRunning
				if existingJob.StartedAt == nil {
					now := time.Now()
					existingJob.StartedAt = &now
				}
				if err := tx.Save(&existingJob).Error; err != nil {
					return err
				}
				itemCopy := item
				submitItem = &itemCopy
				submitJobID = existingJob.PublicID.String()
				return nil
			}
			var item models.ContentItem
			if err := tx.Where("public_id = ? AND tenant_id = ?", batchItem.ContentItemID, batch.TenantID).First(&item).Error; err != nil {
				return tx.Model(&batchItem).Updates(map[string]interface{}{"status": models.TranscriptionBatchItemStatusFailed, "error": "content item not found"}).Error
			}
			job, triggered, reason, _, err := createTranscriptionJobForItem(tx, &item, models.TranscriptionTriggerBulkManual, batch.Force)
			if err != nil {
				return tx.Model(&batchItem).Updates(map[string]interface{}{"status": models.TranscriptionBatchItemStatusFailed, "error": err.Error()}).Error
			}
			job.BatchID = &batch.PublicID
			job.BatchItemID = &batchItem.PublicID
			if !triggered {
				job.Status = models.TranscriptionJobStatusSkipped
				job.SkipReason = reason
				if err := tx.Save(&job).Error; err != nil {
					return err
				}
				return tx.Model(&batchItem).Updates(map[string]interface{}{"job_id": job.PublicID, "status": models.TranscriptionBatchItemStatusSkipped, "reason": reason}).Error
			}
			job.Status = models.TranscriptionJobStatusRunning
			now := time.Now()
			job.StartedAt = &now
			if err := tx.Save(&job).Error; err != nil {
				return err
			}
			if err := tx.Model(&batchItem).Updates(map[string]interface{}{"job_id": job.PublicID, "status": models.TranscriptionBatchItemStatusAccepted}).Error; err != nil {
				return err
			}
			itemCopy := item
			submitItem = &itemCopy
			submitJobID = job.PublicID.String()
			return nil
		})
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			db.Model(&batchItem).Updates(map[string]interface{}{"status": models.TranscriptionBatchItemStatusFailed, "error": err.Error()})
		}
		if submitItem != nil && submitJobID != "" {
			if err := submitTranscriptionJobToMedia(db, submitItem, submitJobID); err != nil {
				_ = updateTranscriptionJobTerminal(db, submitJobID, models.TranscriptionJobStatusFailed, err.Error())
			}
		}
		recomputeTranscriptionBatch(db, batchID)
	}
	recomputeTranscriptionBatch(db, batchID)
}

func CreateTranscriptionBatch(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req createTranscriptionBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.ContentIDs) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "content_ids are required", Code: "INVALID_REQUEST"})
		return
	}
	force := true
	if req.Force != nil {
		force = *req.Force
	}
	seen := map[uuid.UUID]bool{}
	items := make([]models.TranscriptionBatchItem, 0, len(req.ContentIDs))
	for _, raw := range req.ContentIDs {
		id, err := uuid.Parse(raw)
		if err != nil || seen[id] {
			continue
		}
		seen[id] = true
		items = append(items, models.TranscriptionBatchItem{
			TenantID:      principal.TenantID,
			ContentItemID: id,
			Status:        models.TranscriptionBatchItemStatusPending,
		})
	}
	if len(items) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "No valid content IDs", Code: "INVALID_REQUEST"})
		return
	}
	batch := models.TranscriptionBatch{
		TenantID:   principal.TenantID,
		Status:     models.TranscriptionBatchStatusQueued,
		Force:      force,
		Actor:      principal.Email,
		TotalCount: len(items),
	}
	if err := db.Create(&batch).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create batch", Code: "CREATE_FAILED"})
		return
	}
	for i := range items {
		items[i].BatchID = batch.PublicID
	}
	if err := db.Create(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create batch items", Code: "CREATE_FAILED"})
		return
	}
	go dispatchTranscriptionBatch(db, batch.PublicID)
	c.JSON(http.StatusAccepted, utils.ResponseMessage{
		Code:    http.StatusAccepted,
		Message: "Transcription batch accepted",
		Data:    mapTranscriptionBatch(batch, items),
	})
}

func GetTranscriptionBatch(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid batch ID", Code: "INVALID_ID"})
		return
	}
	var batch models.TranscriptionBatch
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&batch).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Batch not found", Code: "NOT_FOUND"})
		return
	}
	var items []models.TranscriptionBatchItem
	db.Where("batch_id = ? AND tenant_id = ?", batch.PublicID, principal.TenantID).
		Order("created_at ASC").Find(&items)
	jobs := loadTranscriptionJobSummariesForBatchItems(db, principal.TenantID, items)
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Transcription batch fetched", Data: mapTranscriptionBatchWithJobs(batch, items, jobs)})
}

func ListTranscriptionBatches(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	page := 1
	limit := 20
	if raw := strings.TrimSpace(c.Query("page")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			page = parsed
		}
	}
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 100 {
		limit = 100
	}
	query := db.Model(&models.TranscriptionBatch{}).Where("tenant_id = ?", principal.TenantID)
	switch strings.ToLower(strings.TrimSpace(c.Query("status"))) {
	case "", "all":
	case "active":
		query = query.Where("status IN ?", []string{models.TranscriptionBatchStatusQueued, models.TranscriptionBatchStatusRunning})
	case "terminal":
		query = query.Where("status IN ?", []string{models.TranscriptionBatchStatusCompleted, models.TranscriptionBatchStatusCanceled, models.TranscriptionBatchStatusFailed})
	default:
		query = query.Where("status = ?", strings.TrimSpace(c.Query("status")))
	}
	var total int64
	query.Count(&total)
	var batches []models.TranscriptionBatch
	query.Order("created_at DESC").Offset((page - 1) * limit).Limit(limit).Find(&batches)
	out := make([]transcriptionBatchResponse, 0, len(batches))
	for _, batch := range batches {
		var items []models.TranscriptionBatchItem
		db.Where("batch_id = ? AND tenant_id = ?", batch.PublicID, principal.TenantID).
			Order("updated_at DESC").
			Limit(10).
			Find(&items)
		jobs := loadTranscriptionJobSummariesForBatchItems(db, principal.TenantID, items)
		out = append(out, mapTranscriptionBatchWithJobs(batch, items, jobs))
	}
	totalPages := 0
	if total > 0 {
		totalPages = int(math.Ceil(float64(total) / float64(limit)))
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{
		Code:    http.StatusOK,
		Message: "Transcription batches fetched",
		Data: transcriptionBatchListResponse{
			Data:       out,
			Total:      total,
			Page:       page,
			Limit:      limit,
			TotalPages: totalPages,
		},
	})
}

func CancelTranscriptionBatch(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid batch ID", Code: "INVALID_ID"})
		return
	}
	var batch models.TranscriptionBatch
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&batch).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Batch not found", Code: "NOT_FOUND"})
		return
	}
	now := time.Now()
	batch.Status = models.TranscriptionBatchStatusCanceled
	batch.CanceledAt = &now
	_ = db.Save(&batch).Error
	db.Model(&models.TranscriptionBatchItem{}).
		Where("batch_id = ? AND status = ?", batch.PublicID, models.TranscriptionBatchItemStatusPending).
		Updates(map[string]interface{}{"status": models.TranscriptionBatchItemStatusCanceled, "reason": "batch canceled"})

	var jobs []models.TranscriptionJob
	db.Where("batch_id = ? AND status IN ?", batch.PublicID, []string{models.TranscriptionJobStatusQueued, models.TranscriptionJobStatusRunning}).Find(&jobs)
	for _, job := range jobs {
		job.Canceled = true
		if job.Status == models.TranscriptionJobStatusQueued {
			job.Status = models.TranscriptionJobStatusCanceled
			job.SkipReason = "batch canceled"
			job.CompletedAt = &now
			settleTranscriptionBudget(db, job.TenantID, job.ReservedCostUsd, 0)
		}
		_ = db.Save(&job).Error
		if job.BatchItemID != nil {
			db.Model(&models.TranscriptionBatchItem{}).
				Where("public_id = ?", *job.BatchItemID).
				Updates(map[string]interface{}{"status": models.TranscriptionBatchItemStatusCanceled, "reason": "batch canceled"})
		}
		if job.Status == models.TranscriptionJobStatusCanceled {
			updateBatchItemForJob(db, &job)
		}
		if job.MediaJobID != "" {
			_ = cancelMediaTranscriptionJob(job.MediaJobID)
		}
	}
	recomputeTranscriptionBatch(db, batch.PublicID)
	var items []models.TranscriptionBatchItem
	db.Where("batch_id = ? AND tenant_id = ?", batch.PublicID, principal.TenantID).Order("created_at ASC").Find(&items)
	_ = db.Where("public_id = ?", batch.PublicID).First(&batch).Error
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Transcription batch canceled", Data: mapTranscriptionBatch(batch, items)})
}

func ResumeTranscriptionBatches(db *gorm.DB) {
	var batches []models.TranscriptionBatch
	db.Where("status IN ?", []string{models.TranscriptionBatchStatusQueued, models.TranscriptionBatchStatusRunning}).Find(&batches)
	for _, batch := range batches {
		batchID := batch.PublicID
		go dispatchTranscriptionBatch(db, batchID)
	}
}

func BulkCreateTranscriptionJobs(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req bulkTranscriptionJobRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.ContentIDs) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "content_ids are required", Code: "INVALID_REQUEST"})
		return
	}
	results := []gin.H{}
	accepted, skipped, failed := 0, 0, 0
	for _, rawID := range req.ContentIDs {
		id, err := uuid.Parse(rawID)
		if err != nil {
			failed++
			results = append(results, gin.H{"content_id": rawID, "status": "failed", "error": "invalid id"})
			continue
		}
		var item models.ContentItem
		if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&item).Error; err != nil {
			failed++
			results = append(results, gin.H{"content_id": rawID, "status": "failed", "error": "not found"})
			continue
		}
		job, triggered, reason, _, err := createTranscriptionJobForItem(db, &item, models.TranscriptionTriggerBulkManual, req.Force)
		if err != nil {
			failed++
			results = append(results, gin.H{"content_id": rawID, "status": "failed", "error": err.Error()})
			continue
		}
		if !triggered {
			skipped++
			results = append(results, gin.H{"content_id": rawID, "status": "skipped", "reason": reason, "job_id": job.PublicID.String()})
			continue
		}
		accepted++
		results = append(results, gin.H{"content_id": rawID, "status": "accepted", "job_id": job.PublicID.String()})
		itemCopy := item
		jobID := job.PublicID.String()
		go func() {
			if err := submitTranscriptionJobToMedia(db, &itemCopy, jobID); err != nil {
				_ = updateTranscriptionJobTerminal(db, jobID, models.TranscriptionJobStatusFailed, err.Error())
			}
		}()
	}
	c.JSON(http.StatusAccepted, utils.ResponseMessage{
		Code:    http.StatusAccepted,
		Message: "Bulk transcription jobs processed",
		Data: gin.H{
			"accepted": accepted,
			"skipped":  skipped,
			"failed":   failed,
			"results":  results,
		},
	})
}

func ListTranscriptionJobs(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 100 {
		limit = 100
	}
	var jobs []models.TranscriptionJob
	query := db.Where("tenant_id = ?", principal.TenantID)
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		query = query.Where("status = ?", status)
	}
	if contentID := strings.TrimSpace(c.Query("content_id")); contentID != "" {
		if id, err := uuid.Parse(contentID); err == nil {
			query = query.Where("content_item_id = ?", id)
		}
	}
	query.Order("created_at DESC").Limit(limit).Find(&jobs)
	contentIDs := make([]uuid.UUID, 0, len(jobs))
	for _, job := range jobs {
		contentIDs = append(contentIDs, job.ContentItemID)
	}
	var contentItems []models.ContentItem
	if len(contentIDs) > 0 {
		db.Where("tenant_id = ? AND public_id IN ?", principal.TenantID, contentIDs).Find(&contentItems)
	}
	contentByID := map[uuid.UUID]models.ContentItem{}
	for _, item := range contentItems {
		contentByID[item.PublicID] = item
	}
	out := make([]transcriptionJobResponse, 0, len(jobs))
	for _, job := range jobs {
		item, ok := contentByID[job.ContentItemID]
		if ok {
			out = append(out, mapTranscriptionJobWithContent(job, &item))
		} else {
			out = append(out, mapTranscriptionJob(job))
		}
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Transcription jobs fetched", Data: out})
}

func ListTranscriptQuality(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.TranscriptQuality
	query := db.Where("tenant_id = ?", principal.TenantID)
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		query = query.Where("status = ?", status)
	}
	query.Order("computed_at DESC").Limit(100).Find(&rows)
	out := make([]transcriptQualityResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, mapTranscriptQuality(row))
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Transcript quality fetched", Data: out})
}

type transcriptionRepairSweepResponse struct {
	Accepted int              `json:"accepted"`
	Skipped  int              `json:"skipped"`
	Failed   int              `json:"failed"`
	Reasons  map[string]int   `json:"reasons"`
	Results  []map[string]any `json:"results"`
	JobIDs   []string         `json:"job_ids"`
}

func activeTranscriptIsProtected(db *gorm.DB, item models.ContentItem) (bool, string) {
	if item.TranscriptID == nil {
		return false, ""
	}
	var transcript models.Transcript
	if err := db.Where("public_id = ?", *item.TranscriptID).First(&transcript).Error; err != nil {
		return false, ""
	}
	if transcript.ApprovedAt != nil {
		return true, "approved transcript"
	}
	if transcript.Source != nil && *transcript.Source == models.TranscriptSourceYouTubeHuman {
		return true, "human transcript"
	}
	return false, ""
}

func hasActiveTranscriptionJob(db *gorm.DB, contentID uuid.UUID) bool {
	var count int64
	db.Model(&models.TranscriptionJob{}).
		Where("content_item_id = ? AND status IN ?", contentID, []string{models.TranscriptionJobStatusQueued, models.TranscriptionJobStatusRunning}).
		Count(&count)
	return count > 0
}

func RepairTranscriptionQualitySweep(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := 100
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 250 {
		limit = 250
	}
	cfg := getOrCreateTranscriptionConfig(db, principal.TenantID)
	resp := transcriptionRepairSweepResponse{
		Reasons: map[string]int{},
		Results: []map[string]any{},
		JobIDs:  []string{},
	}
	if !cfg.AutoRepairEnabled {
		resp.Reasons["auto repair disabled"] = 1
		c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Repair sweep skipped", Data: resp})
		return
	}
	var items []models.ContentItem
	db.Where("tenant_id = ? AND type IN ? AND media_url IS NOT NULL AND media_url <> ''", principal.TenantID, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where(`(
			transcript_id IS NULL
			OR COALESCE(caption_state, '') IN ?
			OR EXISTS (
				SELECT 1 FROM transcript_quality tq
				WHERE tq.content_item_id = content_items.public_id AND tq.status = ?
			)
		)`, []string{"", models.CaptionStateNone, models.CaptionStateYouTubeAuto}, models.TranscriptQualityAutoRepair).
		Order("updated_at DESC").
		Limit(limit).
		Find(&items)

	type pendingSubmit struct {
		item  models.ContentItem
		jobID string
	}
	var submits []pendingSubmit
	for _, item := range items {
		contentID := item.PublicID.String()
		if protected, reason := activeTranscriptIsProtected(db, item); protected {
			resp.Skipped++
			resp.Reasons[reason]++
			resp.Results = append(resp.Results, map[string]any{"content_id": contentID, "status": "skipped", "reason": reason})
			continue
		}
		if hasActiveTranscriptionJob(db, item.PublicID) {
			reason := "transcription already queued or running"
			resp.Skipped++
			resp.Reasons[reason]++
			resp.Results = append(resp.Results, map[string]any{"content_id": contentID, "status": "skipped", "reason": reason})
			continue
		}
		job, triggered, reason, _, err := createTranscriptionJobForItem(db, &item, models.TranscriptionTriggerAutoQuality, false)
		if err != nil {
			resp.Failed++
			resp.Reasons[err.Error()]++
			resp.Results = append(resp.Results, map[string]any{"content_id": contentID, "status": "failed", "error": err.Error()})
			continue
		}
		if !triggered {
			resp.Skipped++
			resp.Reasons[reason]++
			resp.Results = append(resp.Results, map[string]any{"content_id": contentID, "status": "skipped", "reason": reason, "job_id": job.PublicID.String()})
			continue
		}
		resp.Accepted++
		resp.JobIDs = append(resp.JobIDs, job.PublicID.String())
		resp.Results = append(resp.Results, map[string]any{"content_id": contentID, "status": "accepted", "job_id": job.PublicID.String()})
		submits = append(submits, pendingSubmit{item: item, jobID: job.PublicID.String()})
	}
	// Submit accepted jobs to Media from a single background goroutine, one at a
	// time. Spawning a goroutine per item would fire up to `limit` concurrent
	// submits, risking connection-pool exhaustion and overwhelming Media; the
	// batch dispatcher submits sequentially for the same reason.
	if len(submits) > 0 {
		go func() {
			for _, s := range submits {
				itemCopy := s.item
				if err := submitTranscriptionJobToMedia(db, &itemCopy, s.jobID); err != nil {
					_ = updateTranscriptionJobTerminal(db, s.jobID, models.TranscriptionJobStatusFailed, err.Error())
				}
			}
		}()
	}
	c.JSON(http.StatusAccepted, utils.ResponseMessage{Code: http.StatusAccepted, Message: "Repair sweep processed", Data: resp})
}

func updateTranscriptionJobTerminal(db *gorm.DB, jobID, status, message string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return err
	}
	var job models.TranscriptionJob
	if err := db.Where("public_id = ?", id).First(&job).Error; err != nil {
		return err
	}
	if terminalTranscriptionStatus(job.Status) && job.CompletedAt != nil {
		return nil
	}
	now := time.Now()
	job.Status = status
	job.ErrorMessage = message
	job.CompletedAt = &now
	if err := db.Save(&job).Error; err != nil {
		return err
	}
	settleTranscriptionBudget(db, job.TenantID, job.ReservedCostUsd, 0)
	updateBatchItemForJob(db, &job)
	return nil
}
