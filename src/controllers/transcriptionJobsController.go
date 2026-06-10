package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type transcriptionJobResponse struct {
	ID               string                 `json:"id"`
	ContentItemID    string                 `json:"content_item_id"`
	TranscriptID     *string                `json:"transcript_id,omitempty"`
	TriggerSource    string                 `json:"trigger_source"`
	Status           string                 `json:"status"`
	Provider         string                 `json:"provider,omitempty"`
	Model            string                 `json:"model,omitempty"`
	Language         string                 `json:"language,omitempty"`
	SkipReason       string                 `json:"skip_reason,omitempty"`
	ErrorMessage     string                 `json:"error_message,omitempty"`
	RetryCount       int                    `json:"retry_count"`
	EstimatedCostUsd float64                `json:"estimated_cost_usd"`
	ReservedCostUsd  float64                `json:"reserved_cost_usd"`
	ActualCostUsd    float64                `json:"actual_cost_usd"`
	StartedAt        *string                `json:"started_at,omitempty"`
	CompletedAt      *string                `json:"completed_at,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt        string                 `json:"created_at"`
	UpdatedAt        string                 `json:"updated_at"`
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
		models.TranscriptionJobStatusWritebackFailed:
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
	return transcriptionJobResponse{
		ID:               job.PublicID.String(),
		ContentItemID:    job.ContentItemID.String(),
		TranscriptID:     transcriptID,
		TriggerSource:    job.TriggerSource,
		Status:           job.Status,
		Provider:         job.Provider,
		Model:            job.Model,
		Language:         job.Language,
		SkipReason:       job.SkipReason,
		ErrorMessage:     job.ErrorMessage,
		RetryCount:       job.RetryCount,
		EstimatedCostUsd: job.EstimatedCostUsd,
		ReservedCostUsd:  job.ReservedCostUsd,
		ActualCostUsd:    job.ActualCostUsd,
		StartedAt:        formatTimePtr(job.StartedAt),
		CompletedAt:      formatTimePtr(job.CompletedAt),
		Metadata:         meta,
		CreatedAt:        job.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:        job.UpdatedAt.UTC().Format(time.RFC3339),
	}
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

func reserveTranscriptionBudget(db *gorm.DB, tenantID string, est float64) {
	if est <= 0 {
		return
	}
	db.Model(&models.TranscriptionConfig{}).
		Where("tenant_id = ?", tenantID).
		UpdateColumn("monthly_reserved_usd", gorm.Expr("monthly_reserved_usd + ?", est))
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
	job := models.TranscriptionJob{
		TenantID:         item.TenantID,
		ContentItemID:    item.PublicID,
		TranscriptID:     item.TranscriptID,
		TriggerSource:    triggerSource,
		Status:           models.TranscriptionJobStatusQueued,
		EstimatedCostUsd: est,
		ReservedCostUsd:  est,
	}
	if err := db.Create(&job).Error; err != nil {
		return job, err
	}
	reserveTranscriptionBudget(db, item.TenantID, est)
	return job, nil
}

func latestTranscriptQuality(db *gorm.DB, contentID uuid.UUID) *models.TranscriptQuality {
	var q models.TranscriptQuality
	if err := db.Where("content_item_id = ?", contentID).First(&q).Error; err != nil {
		return nil
	}
	return &q
}

func createTranscriptionJobForItem(db *gorm.DB, item *models.ContentItem, triggerSource string, force bool) (models.TranscriptionJob, bool, string, error) {
	cfg := getOrCreateTranscriptionConfig(db, item.TenantID)
	state := ""
	if item.CaptionState != nil {
		state = *item.CaptionState
	}

	if triggerSource == "" {
		triggerSource = models.TranscriptionTriggerManual
	}

	if item.MediaURL == nil || strings.TrimSpace(*item.MediaURL) == "" {
		job := createSkippedTranscriptionJob(db, item, triggerSource, "no media_url available")
		return job, false, job.SkipReason, nil
	}

	if !force {
		if state == models.CaptionStateYouTubeHuman {
			job := createSkippedTranscriptionJob(db, item, triggerSource, "human caption present (no STT needed)")
			return job, false, job.SkipReason, nil
		}
		if state == models.CaptionStateSTTDone {
			q := latestTranscriptQuality(db, item.PublicID)
			if q == nil || q.Status != models.TranscriptQualityAutoRepair || !cfg.AutoRepairEnabled {
				job := createSkippedTranscriptionJob(db, item, triggerSource, "already upgraded by STT")
				return job, false, job.SkipReason, nil
			}
			triggerSource = models.TranscriptionTriggerAutoQuality
		}
		if state == models.CaptionStateYouTubeAuto && !cfg.AutoSttEnabled && triggerSource != models.TranscriptionTriggerAutoQuality {
			job := createSkippedTranscriptionJob(db, item, triggerSource, "auto-STT disabled (manual trigger required)")
			return job, false, job.SkipReason, nil
		}
		if state == "" || state == models.CaptionStateNone {
			if !cfg.AutoSttEnabled && triggerSource != models.TranscriptionTriggerManual && triggerSource != models.TranscriptionTriggerBulkManual {
				job := createSkippedTranscriptionJob(db, item, triggerSource, "auto-STT disabled (manual trigger required)")
				return job, false, job.SkipReason, nil
			}
		}
	}

	est := estimateSTTCostUSD(item.DurationSec)
	if cfg.MonthlyBudgetCapUsd > 0 && cfg.MonthlySpendUsd+cfg.MonthlyReservedUsd+est > cfg.MonthlyBudgetCapUsd {
		job := createSkippedTranscriptionJob(db, item, triggerSource, "monthly STT budget cap reached")
		return job, false, job.SkipReason, nil
	}

	job, err := createAcceptedTranscriptionJob(db, item, triggerSource)
	if err != nil {
		return job, false, "", err
	}
	return job, true, "", nil
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
		TenantID:       tenantID,
		ContentItemID:  item.PublicID,
		TranscriptID:   existing.PublicID,
		FullText:       existing.FullText,
		Summary:        existing.Summary,
		WordTimestamps: existing.WordTimestamps,
		Segments:       existing.Segments,
		Chapters:       existing.Chapters,
		Language:       existing.Language,
		Source:         existing.Source,
		Provider:       existing.Provider,
		Checksum:       checksumTranscriptText(existing.FullText, existing.Segments),
		Reason:         "stt_replacement",
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
	if req.RetryCount != nil {
		job.RetryCount = *req.RetryCount
	}
	if req.TranscriptID != nil {
		if id, err := uuid.Parse(*req.TranscriptID); err == nil {
			job.TranscriptID = &id
		}
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
	Status        *string                `json:"status"`
	Provider      *string                `json:"provider"`
	Model         *string                `json:"model"`
	Language      *string                `json:"language"`
	SkipReason    *string                `json:"skip_reason"`
	ErrorMessage  *string                `json:"error_message"`
	RetryCount    *int                   `json:"retry_count"`
	TranscriptID  *string                `json:"transcript_id"`
	DurationSec   *float64               `json:"duration_sec"`
	ActualCostUsd *float64               `json:"actual_cost_usd"`
	Metadata      map[string]interface{} `json:"metadata"`
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
		if job.Status == models.TranscriptionJobStatusSucceeded || job.Status == models.TranscriptionJobStatusWritebackFailed {
			actual = job.ActualCostUsd
			if actual == 0 {
				actual = job.EstimatedCostUsd
			}
		}
		settleTranscriptionBudget(db, job.TenantID, job.ReservedCostUsd, actual)
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
	job, triggered, reason, err := createTranscriptionJobForItem(db, &item, trigger, req.Force)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to create transcription job", Code: "CREATE_FAILED"})
		return
	}
	if triggered {
		itemCopy := item
		jobID := job.PublicID.String()
		go func() {
			if err := triggerTranscriptionForJob(&itemCopy, jobID); err != nil {
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
		job, triggered, reason, err := createTranscriptionJobForItem(db, &item, models.TranscriptionTriggerBulkManual, req.Force)
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
			if err := triggerTranscriptionForJob(&itemCopy, jobID); err != nil {
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
	out := make([]transcriptionJobResponse, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, mapTranscriptionJob(job))
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
	return nil
}
