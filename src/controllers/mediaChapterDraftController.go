package controllers

import (
	"content-management-system/src/contentstage"
	"content-management-system/src/models"
	"content-management-system/src/podsflow"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"math"
	"net/http"
	"strings"
	"time"
)

func draftSchema(db *gorm.DB) bool { return db.Migrator().HasTable(&models.MediaChapterDraft{}) }

// The editor uses the same lock order as admission. It must not change input
// beneath a claim or replace the editorial rows belonging to frozen cuts.
func guardStudioEdit(tx *gorm.DB, item models.ContentItem, chapterRows bool) error {
	if item.ParentContentItemID != nil {
		return fmt.Errorf("Generated chapter inputs are governed by their episode; open the parent episode to edit")
	}
	var stages []models.ContentStageRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND content_item_id=? AND processing_generation=?", item.TenantID, item.PublicID, item.ProcessingGeneration).Order("public_id ASC").Find(&stages).Error; err != nil {
		return err
	}
	var current models.ContentItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", item.TenantID, item.PublicID).First(&current).Error; err != nil {
		return err
	}
	if current.ProcessingGeneration != item.ProcessingGeneration {
		return fmt.Errorf("Episode changed; reload before editing")
	}
	for _, stage := range stages {
		switch stage.State {
		case "claimed", "running", "verifying", "uncertain", "reconciling":
			return fmt.Errorf("Active or unresolved work uses this input; wait for it to settle")
		}
	}
	var generations int64
	q := tx.Model(&models.AtomizationGeneration{}).Where("tenant_id=? AND parent_content_item_id=?", item.TenantID, item.PublicID)
	if !chapterRows {
		q = q.Where("state NOT IN ('active','superseded','failed')")
	}
	if err := q.Count(&generations).Error; err != nil {
		return err
	}
	if generations > 0 {
		return fmt.Errorf("This episode has a frozen plan; use a versioned draft and explicit replacement")
	}
	if chapterRows {
		var linked int64
		if err := tx.Model(&models.Chapter{}).Where("tenant_id=? AND transcript_id=? AND child_content_item_id IS NOT NULL", item.TenantID, item.TranscriptID).Count(&linked).Error; err != nil {
			return err
		}
		if linked > 0 {
			return fmt.Errorf("Published chapter links cannot be replaced; save a chapter draft")
		}
	}
	unresolved, err := podsflow.HasUnresolvedChapterEffects(tx, item)
	if err != nil {
		return err
	}
	if unresolved {
		return fmt.Errorf("Media effects require reconciliation before editing")
	}
	return nil
}

func draftInputs(db *gorm.DB, p models.ContentItem) (string, string, string, error) {
	if p.TranscriptID == nil || p.DurationSec == nil || *p.DurationSec <= 2400 {
		return "", "", "", fmt.Errorf("A long episode with a timestamped transcript is required")
	}
	var t models.Transcript
	if err := episodeTranscriptQuery(db, p).First(&t).Error; err != nil {
		return "", "", "", err
	}
	if len(extractSegments(&t)) == 0 {
		return "", "", "", fmt.Errorf("Timestamped transcript required")
	}
	policy, err := readOnlyEffectiveMediaPolicy(db, &p)
	if err != nil {
		return "", "", "", err
	}
	td, pd := atomizationTranscriptDigest(t), longFormDigest(policy.Policy)
	return td, pd, longFormDigest([]any{p.PublicID, p.ProcessingGeneration, draftSourceDigest(p), td, pd}), nil
}

func draftSourceDigest(p models.ContentItem) string {
	return longFormDigest([]any{p.DurationSec, p.MediaVersion, p.MediaURL, p.OriginalURL})
}

func validateDraft(plan []map[string]any, duration int) []string {
	errors := []string{}
	if len(plan) == 0 {
		return []string{"At least one chapter is required"}
	}
	end := float64(0)
	for i, ch := range plan {
		start, ok1 := ch["start_ms"].(float64)
		stop, ok2 := ch["end_ms"].(float64)
		title, _ := ch["title"].(string)
		if strings.TrimSpace(title) == "" {
			errors = append(errors, fmt.Sprintf("Chapter %d needs a title", i+1))
		}
		if !ok1 || !ok2 || math.Trunc(start) != start || math.Trunc(stop) != stop || start != end || stop-start < 270000 || stop-start > 2400000 {
			errors = append(errors, fmt.Sprintf("Chapter %d must cover the next contiguous 4:30–40:00 interval", i+1))
		}
		end = stop
	}
	if end != float64(duration)*1000 {
		errors = append(errors, "Plan must cover the complete episode")
	}
	return errors
}

// Draft input is editorial content, never worker output or publication proof.
func normalizeDraft(plan []map[string]any) []map[string]any {
	result := make([]map[string]any, 0, len(plan))
	for _, chapter := range plan {
		title, _ := chapter["title"].(string)
		result = append(result, map[string]any{
			"source":   "manual",
			"title":    strings.TrimSpace(title),
			"start_ms": chapter["start_ms"],
			"end_ms":   chapter["end_ms"],
		})
	}
	return result
}

func draftWorkerReady(db *gorm.DB) bool {
	var n int64
	return db.Table("media_worker_capabilities").Where("name='chapter_plan' AND version=1 AND observed_at>?", time.Now().UTC().Add(-5*time.Minute)).Count(&n).Error == nil && n > 0
}

// This is advisory, read-only eligibility. Apply repeats the checks under locks.
func draftApplyBlock(db *gorm.DB, p models.ContentItem, stages []models.ContentStageRequest) (string, error) {
	verified := map[string]bool{}
	var request *models.ContentStageRequest
	for _, stage := range stages {
		verified[stage.Stage] = stage.State == models.ContentStageVerified
		if stage.Stage == models.ContentStagePodsAtomization {
			copy := stage
			request = &copy
		}
		switch stage.State {
		case "uncertain", "reconciling":
			return "effects_unresolved", nil
		case "claimed", "running", "verifying":
			return "effects_active", nil
		}
	}
	if request == nil || !verified[models.ContentStagePodsMediaArtifacts] || !verified[models.ContentStagePodsTranscript] {
		return "prerequisites_required", nil
	}
	if request.State != models.ContentStageVerified && request.State != models.ContentStageFailed {
		var frozen int64
		if err := db.Model(&models.AtomizationGeneration{}).Where("tenant_id=? AND content_stage_request_id=?", p.TenantID, request.PublicID).Count(&frozen).Error; err != nil {
			return "", err
		}
		if frozen > 0 {
			return "frozen_plan_must_settle", nil
		}
	}
	var uncertainFailures int64
	if err := db.Table("atomization_chapter_units u").Joins("JOIN atomization_generations g ON g.tenant_id=u.tenant_id AND g.public_id=u.generation_id").Where("g.tenant_id=? AND g.parent_content_item_id=? AND u.state='failed' AND u.failure_class<>'verified_absent'", p.TenantID, p.PublicID).Count(&uncertainFailures).Error; err != nil {
		return "", err
	}
	if uncertainFailures > 0 {
		return "effects_unresolved", nil
	}
	unresolved, err := podsflow.HasUnresolvedChapterEffects(db, p)
	if err != nil {
		return "", err
	}
	if unresolved {
		return "effects_unresolved", nil
	}
	policy, err := readOnlyEffectiveMediaPolicy(db, &p)
	if err != nil {
		return "", err
	}
	if !policy.Policy.ChapteringEnabled {
		return "policy_disabled", nil
	}
	return "", nil
}

func AdminListMediaChapterDrafts(c *gin.Context) {
	db, p, ok := journeyParent(c)
	if !ok {
		return
	}
	if !draftSchema(db) {
		c.JSON(503, utils.HTTPError{Code: 503, Message: "Chapter drafts require the CMS migration"})
		return
	}
	rows := []models.MediaChapterDraft{}
	if err := db.Where("tenant_id=? AND parent_content_item_id=?", p.TenantID, p.PublicID).Order("revision DESC").Limit(25).Find(&rows).Error; err != nil {
		mediaAtomizationQueryError(c, err)
		return
	}
	_, _, fingerprint, inputErr := draftInputs(db, *p)
	worker := draftWorkerReady(db)
	principal, _ := requireAdminPrincipal(c)
	reason := ""
	switch {
	case !principal.HasPermission("content:write"):
		reason = "permission_required"
	case inputErr != nil:
		reason = "draft_inputs_unavailable"
	case !worker:
		reason = "draft_worker_required"
	default:
		var stages []models.ContentStageRequest
		if err := db.Select("public_id", "stage", "state").Where("tenant_id=? AND content_item_id=? AND processing_generation=?", p.TenantID, p.PublicID, p.ProcessingGeneration).Find(&stages).Error; err != nil {
			mediaAtomizationQueryError(c, err)
			return
		}
		var err error
		reason, err = draftApplyBlock(db, *p, stages)
		if err != nil {
			mediaAtomizationQueryError(c, err)
			return
		}
	}
	if reason == "" && len(rows) > 0 {
		switch {
		case rows[0].AppliedRequestID != nil:
			reason = "draft_already_applied"
		case rows[0].InputFingerprint != fingerprint:
			reason = "draft_inputs_changed"
		case string(rows[0].Validation) != "[]":
			reason = "draft_invalid"
		}
	}
	c.JSON(200, utils.ResponseMessage{Code: 200, Data: gin.H{"items": rows, "input_fingerprint": fingerprint, "worker_supported": worker, "apply_action": mediaJourneyAction{Code: "apply_chapter_plan", Step: "planning", Enabled: reason == "", Reason: reason}}})
}

func AdminSaveMediaChapterDraft(c *gin.Context) {
	db, p, ok := journeyParent(c)
	if !ok {
		return
	}
	if !draftSchema(db) {
		c.JSON(503, utils.HTTPError{Code: 503, Message: "Chapter drafts require the CMS migration"})
		return
	}
	var body struct {
		ExpectedRevision *int64           `json:"expected_revision"`
		Chapters         []map[string]any `json:"chapters"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	if c.ShouldBindJSON(&body) != nil || body.ExpectedRevision == nil || *body.ExpectedRevision < 0 {
		c.JSON(400, utils.HTTPError{Code: 400, Message: "Invalid chapter draft (maximum 1 MiB)"})
		return
	}
	body.Chapters = normalizeDraft(body.Chapters)
	principal, _ := requireAdminPrincipal(c)
	var draft models.MediaChapterDraft
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", p.TenantID, p.PublicID).First(p).Error; err != nil {
			return err
		}
		var revision int64
		if err := tx.Model(&models.MediaChapterDraft{}).Where("tenant_id=? AND parent_content_item_id=?", p.TenantID, p.PublicID).Select("COALESCE(MAX(revision),0)").Scan(&revision).Error; err != nil {
			return err
		}
		if revision != *body.ExpectedRevision {
			return fmt.Errorf("Draft changed; reload before saving")
		}
		td, pd, fp, err := draftInputs(tx, *p)
		if err != nil {
			return err
		}
		draft = models.MediaChapterDraft{PublicID: uuid.New(), TenantID: p.TenantID, ParentContentItemID: p.PublicID, Revision: revision + 1, BaseProcessingGeneration: p.ProcessingGeneration, InputFingerprint: fp, TranscriptDigest: td, PolicyDigest: pd, Plan: longFormJSON(body.Chapters), Provenance: "manual", Author: principal.UserID, Validation: longFormJSON(validateDraft(body.Chapters, *p.DurationSec)), CreatedAt: time.Now().UTC()}
		return tx.Create(&draft).Error
	})
	if err != nil {
		c.JSON(409, utils.HTTPError{Code: 409, Message: err.Error()})
		return
	}
	c.JSON(201, utils.ResponseMessage{Code: 201, Message: "Draft saved; processing is unchanged", Data: draft})
}

func AdminApplyMediaChapterDraft(c *gin.Context) {
	db, p, ok := journeyParent(c)
	if !ok {
		return
	}
	if !draftSchema(db) {
		c.JSON(503, utils.HTTPError{Code: 503, Message: "Chapter drafts require the CMS migration"})
		return
	}
	id, err := uuid.Parse(c.Param("planId"))
	if err != nil {
		c.JSON(400, utils.HTTPError{Code: 400, Message: "Invalid plan ID"})
		return
	}
	var body struct {
		Revision    int64     `json:"revision"`
		Fingerprint string    `json:"input_fingerprint"`
		Key         uuid.UUID `json:"idempotency_key"`
	}
	if c.ShouldBindJSON(&body) != nil || body.Key == uuid.Nil {
		c.JSON(400, utils.HTTPError{Code: 400, Message: "Revision, fingerprint and idempotency key required"})
		return
	}
	principal, _ := requireAdminPrincipal(c)
	var result models.ContentStageRequest
	err = db.Transaction(func(tx *gorm.DB) error {
		// Match admission lock ordering: stages, then root, then draft.
		var stages []models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND content_item_id=? AND processing_generation=?", p.TenantID, p.PublicID, p.ProcessingGeneration).Order("public_id ASC").Find(&stages).Error; err != nil {
			return err
		}
		base := p.ProcessingGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", p.TenantID, p.PublicID).First(p).Error; err != nil {
			return err
		}
		var d models.MediaChapterDraft
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND parent_content_item_id=? AND public_id=?", p.TenantID, p.PublicID, id).First(&d).Error; err != nil {
			return err
		}
		if d.AppliedRequestID != nil {
			if d.ApplicationKey == nil || *d.ApplicationKey != body.Key || d.Revision != body.Revision || body.Fingerprint != d.InputFingerprint {
				return fmt.Errorf("Draft already applied with a different request")
			}
			return tx.Where("tenant_id=? AND public_id=?", p.TenantID, d.AppliedRequestID).First(&result).Error
		}
		if p.ProcessingGeneration != base {
			return fmt.Errorf("Episode changed; reload before applying")
		}
		if !draftWorkerReady(tx) {
			return fmt.Errorf("A draft-aware worker must be online before applying")
		}
		td, pd, fp, err := draftInputs(tx, *p)
		if err != nil {
			return err
		}
		if d.Revision != body.Revision || body.Fingerprint != fp || d.InputFingerprint != fp || d.TranscriptDigest != td || d.PolicyDigest != pd {
			return fmt.Errorf("Episode, transcript or policy changed; save a revalidated draft")
		}
		if reason, err := draftApplyBlock(tx, *p, stages); err != nil {
			return err
		} else if reason != "" {
			return fmt.Errorf("Cannot apply chapter plan: %s", reason)
		}
		var plan []map[string]any
		if json.Unmarshal(d.Plan, &plan) != nil {
			return fmt.Errorf("Invalid stored plan")
		}
		if issues := validateDraft(plan, *p.DurationSec); len(issues) > 0 {
			return fmt.Errorf("%s", strings.Join(issues, "; "))
		}
		var handled bool
		result, handled, err = contentstage.RequestManualAtomization(tx, p.TenantID, p.PublicID, principal.UserID, true)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("Durable atomization admission is required")
		}
		// A queued request that already owns a frozen plan must resume it, not replace it.
		var existing int64
		if err := tx.Model(&models.AtomizationGeneration{}).Where("tenant_id=? AND content_stage_request_id=?", p.TenantID, result.PublicID).Count(&existing).Error; err != nil {
			return err
		}
		if existing > 0 {
			return fmt.Errorf("Frozen plan exists; settle this generation before replacement")
		}
		if err := tx.Model(&result).Update("workload_estimate", gorm.Expr("COALESCE(workload_estimate,'{}'::jsonb) || ?::jsonb", string(longFormJSON(gin.H{"chapter_plan_id": d.PublicID, "chapter_plan_version": 1, "chapter_plan_source_digest": draftSourceDigest(*p)})))).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		return tx.Model(&d).Updates(map[string]any{"applied_request_id": result.PublicID, "applied_processing_generation": result.ProcessingGeneration, "application_key": body.Key, "applied_at": now}).Error
	})
	if err != nil {
		c.JSON(409, utils.HTTPError{Code: 409, Message: err.Error()})
		return
	}
	c.JSON(202, utils.ResponseMessage{Code: 202, Message: "Applied plan admitted; published media remains until replacement is ready", Data: gin.H{"request_id": result.PublicID, "state": result.State}})
}
