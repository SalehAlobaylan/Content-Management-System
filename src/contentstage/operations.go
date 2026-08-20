package contentstage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LaneHealth struct {
	Lane             string                      `json:"lane"`
	Cutover          string                      `json:"cutover"`
	Control          models.ContentStageControl  `json:"control"`
	Verdict          string                      `json:"verdict"`
	Reasons          []string                    `json:"reasons"`
	StateCounts      map[string]int64            `json:"state_counts"`
	StageStateCounts map[string]map[string]int64 `json:"stage_state_counts"`
	OldestQueuedAt   *time.Time                  `json:"oldest_queued_at,omitempty"`
	OldestActiveAt   *time.Time                  `json:"oldest_active_at,omitempty"`
}

// currentStageRequests excludes historical processing generations. Superseded
// generations remain available in traces and audit events, but must not make a
// healthy current generation look permanently degraded or unqualifiable.
func currentStageRequests(db *gorm.DB, tenantID, lane string) *gorm.DB {
	return db.Table("content_stage_requests AS csr").
		Joins("JOIN content_items ci ON ci.tenant_id=csr.tenant_id AND ci.public_id=csr.content_item_id AND ci.processing_generation=csr.processing_generation").
		Where("csr.tenant_id=? AND csr.lane=? AND ci.status<>?", tenantID, lane, models.ContentStatusArchived)
}

func Health(db *gorm.DB, tenantID string) ([]LaneHealth, error) {
	out := make([]LaneHealth, 0, 2)
	for _, lane := range []string{models.ContentStageLaneNews, models.ContentStageLanePods} {
		mode, err := CutoverMode(db, tenantID, lane)
		if err != nil {
			return nil, err
		}
		var control models.ContentStageControl
		if err := db.Where("tenant_id=? AND lane=?", tenantID, lane).First(&control).Error; err != nil {
			if err != gorm.ErrRecordNotFound {
				return nil, err
			}
			control = models.ContentStageControl{TenantID: tenantID, Lane: lane, SchedulingEnabled: true, ExecutionEnabled: true, OptionalMetadataEnabled: true, TranscriptExecutionEnabled: true}
		}
		var rows []struct {
			State string
			Count int64
		}
		if err := currentStageRequests(db, tenantID, lane).Select("csr.state, COUNT(*) AS count").Where("csr.blocking_scope<>?", models.ContentStageBlockingOptional).Group("csr.state").Scan(&rows).Error; err != nil {
			return nil, err
		}
		counts := map[string]int64{}
		for _, row := range rows {
			counts[row.State] = row.Count
		}
		var stageRows []struct {
			Stage string
			State string
			Count int64
		}
		if err := currentStageRequests(db, tenantID, lane).Select("csr.stage, csr.state, COUNT(*) AS count").Group("csr.stage, csr.state").Scan(&stageRows).Error; err != nil {
			return nil, err
		}
		stageCounts := map[string]map[string]int64{}
		for _, row := range stageRows {
			if stageCounts[row.Stage] == nil {
				stageCounts[row.Stage] = map[string]int64{}
			}
			stageCounts[row.Stage][row.State] = row.Count
		}
		var oldestQueued, oldestActive *time.Time
		_ = currentStageRequests(db, tenantID, lane).Where("csr.blocking_scope<>? AND csr.state IN ?", models.ContentStageBlockingOptional, []string{models.ContentStageQueued, models.ContentStageDeferred}).Select("MIN(csr.created_at)").Scan(&oldestQueued).Error
		_ = currentStageRequests(db, tenantID, lane).Where("csr.blocking_scope<>? AND csr.state IN ?", models.ContentStageBlockingOptional, []string{models.ContentStageClaimed, models.ContentStageRunning, models.ContentStageVerifying, models.ContentStageUncertain, models.ContentStageReconciling}).Select("MIN(csr.updated_at)").Scan(&oldestActive).Error
		verdict, reasons := "healthy", []string{}
		if mode == models.ContentStageCutoverLegacy {
			verdict, reasons = "legacy", append(reasons, "durable execution is not promoted")
		}
		if !control.SchedulingEnabled || !control.ExecutionEnabled {
			verdict, reasons = "paused", append(reasons, "lane scheduling or execution is paused")
		}
		if mode == models.ContentStageCutoverDurableRequired && (counts[models.ContentStageFailed] > 0 || counts[models.ContentStageUncertain] > 0) {
			verdict, reasons = "degraded", append(reasons, "required stages need operator-visible resolution")
		}
		ageLimit := 5 * time.Minute
		if lane == models.ContentStageLaneNews {
			ageLimit = 60 * time.Second
		}
		if mode == models.ContentStageCutoverDurableRequired && oldestQueued != nil && time.Since(oldestQueued.UTC()) > ageLimit {
			verdict, reasons = "degraded", append(reasons, "required-stage oldest age exceeds lane SLO")
		}
		out = append(out, LaneHealth{Lane: lane, Cutover: mode, Control: control, Verdict: verdict, Reasons: reasons, StateCounts: counts, StageStateCounts: stageCounts, OldestQueuedAt: oldestQueued, OldestActiveAt: oldestActive})
	}
	return out, nil
}

func UpdateControl(db *gorm.DB, tenantID, lane, actor string, scheduling, execution, optionalMetadata, transcript *bool, reason string) (models.ContentStageControl, error) {
	if lane != models.ContentStageLaneNews && lane != models.ContentStageLanePods {
		return models.ContentStageControl{}, fmt.Errorf("invalid lane")
	}
	var value models.ContentStageControl
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", tenantID, lane).First(&value).Error; err != nil {
			if err != gorm.ErrRecordNotFound {
				return err
			}
			value = models.ContentStageControl{TenantID: tenantID, Lane: lane, SchedulingEnabled: true, ExecutionEnabled: true, OptionalMetadataEnabled: true, TranscriptExecutionEnabled: true}
		}
		if scheduling != nil {
			value.SchedulingEnabled = *scheduling
		}
		if execution != nil {
			value.ExecutionEnabled = *execution
		}
		if optionalMetadata != nil {
			value.OptionalMetadataEnabled = *optionalMetadata
		}
		if transcript != nil {
			value.TranscriptExecutionEnabled = *transcript
		}
		value.Reason, value.UpdatedBy = strings.TrimSpace(reason), actor
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "lane"}}, DoUpdates: clause.AssignmentColumns([]string{"scheduling_enabled", "execution_enabled", "optional_metadata_enabled", "transcript_execution_enabled", "reason", "updated_by", "updated_at"})}).Create(&value).Error
	})
	return value, err
}

func Promote(db *gorm.DB, tenantID, lane, mode, actor, verificationDigest string) (models.ContentStageCutover, error) {
	if lane != models.ContentStageLaneNews && lane != models.ContentStageLanePods {
		return models.ContentStageCutover{}, fmt.Errorf("invalid lane")
	}
	if mode != models.ContentStageCutoverLegacy && mode != models.ContentStageCutoverShadow && mode != models.ContentStageCutoverDurableRequired {
		return models.ContentStageCutover{}, fmt.Errorf("invalid mode")
	}
	if mode == models.ContentStageCutoverDurableRequired && strings.TrimSpace(verificationDigest) == "" {
		return models.ContentStageCutover{}, fmt.Errorf("verification digest is required")
	}
	if mode == models.ContentStageCutoverDurableRequired {
		currentMode, err := CutoverMode(db, tenantID, lane)
		if err != nil {
			return models.ContentStageCutover{}, err
		}
		if currentMode != models.ContentStageCutoverShadow {
			return models.ContentStageCutover{}, fmt.Errorf("lane must qualify in shadow before durable promotion")
		}
		expected, err := QualificationDigest(db, tenantID, lane)
		if err != nil {
			return models.ContentStageCutover{}, err
		}
		if strings.TrimSpace(verificationDigest) != expected {
			return models.ContentStageCutover{}, fmt.Errorf("verification digest does not match current shadow evidence")
		}
	}
	now := time.Now().UTC()
	value := models.ContentStageCutover{TenantID: tenantID, Lane: lane, Mode: mode, ProtocolVersion: ProtocolVersion, PromotedBy: actor, VerificationDigest: strings.TrimSpace(verificationDigest)}
	if mode == models.ContentStageCutoverDurableRequired {
		value.PromotedAt = &now
	}
	err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "lane"}}, DoUpdates: clause.AssignmentColumns([]string{"mode", "protocol_version", "promoted_by", "promoted_at", "verification_digest", "updated_at"})}).Create(&value).Error
	return value, err
}

func QualificationDigest(db *gorm.DB, tenantID, lane string) (string, error) {
	var total, unresolved int64
	base := currentStageRequests(db, tenantID, lane).Where("csr.blocking_scope<>?", models.ContentStageBlockingOptional)
	if err := base.Count(&total).Error; err != nil {
		return "", err
	}
	if total == 0 {
		return "", fmt.Errorf("shadow qualification has no required-stage evidence")
	}
	if err := currentStageRequests(db, tenantID, lane).Where("csr.blocking_scope<>? AND csr.state<>?", models.ContentStageBlockingOptional, models.ContentStageVerified).Count(&unresolved).Error; err != nil {
		return "", err
	}
	if unresolved != 0 {
		return "", fmt.Errorf("shadow qualification has %d unresolved required stages", unresolved)
	}
	feedLane, memberType := "news", "story"
	if lane == models.ContentStageLanePods {
		feedLane, memberType = "media", "feed_unit"
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", tenantID, feedLane).First(&head).Error; err != nil || head.ActiveGenerationID == nil {
		return "", fmt.Errorf("active %s feed generation is required for consumer proof", feedLane)
	}
	var missingMembership int64
	if lane == models.ContentStageLaneNews {
		err := db.Raw(`SELECT COUNT(DISTINCT ci.public_id)
			FROM content_items ci JOIN content_stage_requests csr
			  ON csr.tenant_id=ci.tenant_id AND csr.content_item_id=ci.public_id AND csr.processing_generation=ci.processing_generation
			WHERE ci.tenant_id=? AND ci.type='NEWS' AND ci.status='READY' AND csr.stage='news_story_classification' AND csr.state='verified'
			  AND NOT EXISTS (SELECT 1 FROM feed_generation_memberships m WHERE m.generation_id=? AND m.member_type=? AND m.member_id=ci.story_id)`, tenantID, *head.ActiveGenerationID, memberType).Scan(&missingMembership).Error
		if err != nil {
			return "", err
		}
	} else {
		err := db.Raw(`SELECT COUNT(DISTINCT ci.public_id)
			FROM content_items ci JOIN content_stage_requests csr
			  ON csr.tenant_id=ci.tenant_id AND csr.content_item_id=ci.public_id AND csr.processing_generation=ci.processing_generation
			WHERE ci.tenant_id=? AND ci.type IN ('VIDEO','PODCAST') AND ci.status='READY' AND ci.is_feed_unit=true AND ci.feed_visibility='visible'
			  AND csr.stage='pods_text_embedding' AND csr.state='verified'
			  AND NOT EXISTS (SELECT 1 FROM feed_generation_memberships m WHERE m.generation_id=? AND m.member_type=? AND m.member_id=ci.public_id)`, tenantID, *head.ActiveGenerationID, memberType).Scan(&missingMembership).Error
		if err != nil {
			return "", err
		}
	}
	if missingMembership != 0 {
		return "", fmt.Errorf("consumer proof has %d generation-unreachable items", missingMembership)
	}
	var latest time.Time
	if err := currentStageRequests(db, tenantID, lane).Where("csr.blocking_scope<>?", models.ContentStageBlockingOptional).Select("MAX(csr.updated_at)").Scan(&latest).Error; err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%s|%s", ProtocolVersion, tenantID, lane, total, latest.UTC().Format(time.RFC3339Nano), head.ActiveGenerationID.String())))
	return hex.EncodeToString(sum[:]), nil
}

func BackfillCurrent(db *gorm.DB, tenantID, lane string, limit int) (int, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 500 {
		limit = 500
	}
	types := []models.ContentType{models.ContentTypeNews}
	contradiction := "(status=? AND (embedding IS NULL OR story_id IS NULL))"
	if lane == models.ContentStageLanePods {
		types = []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}
		contradiction = "(status=? AND (embedding IS NULL OR playback_url IS NULL OR (duration_sec>2400 AND transcript_id IS NULL)))"
	}
	var items []models.ContentItem
	if err := db.Where("tenant_id=? AND type IN ? AND processing_input_digest IS NULL", tenantID, types).
		Where("status IN ? OR "+contradiction, []models.ContentStatus{models.ContentStatusPending, models.ContentStatusProcessing}, models.ContentStatusReady).
		Order("created_at DESC").Limit(limit).Find(&items).Error; err != nil {
		return 0, err
	}
	created := 0
	for index := range items {
		if err := db.Transaction(func(tx *gorm.DB) error { _, err := EnsureManifest(tx, &items[index]); return err }); err != nil {
			return created, err
		}
		created++
	}
	return created, nil
}
