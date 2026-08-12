package controllers

import (
	"bytes"
	"compress/gzip"
	"content-management-system/src/feedcontract"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const feedRecoveryPlanTTL = 30 * time.Minute

type feedRecoveryPlanRequest struct {
	Lane           string `json:"lane"`
	Level          string `json:"level"`
	CapacityMode   string `json:"capacity_mode"`
	NoFullRollback bool   `json:"no_full_rollback"`
}

type recoveryPurgeManifest struct {
	Version         int         `json:"version"`
	TenantID        string      `json:"tenant_id"`
	Lane            string      `json:"lane"`
	SourceIDs       []uuid.UUID `json:"source_ids"`
	NewsContentIDs  []uuid.UUID `json:"news_content_ids"`
	NewsStoryIDs    []uuid.UUID `json:"news_story_ids"`
	MediaContentIDs []uuid.UUID `json:"media_content_ids"`
	LookbackHours   int         `json:"lookback_hours"`
	NewsMaxItems    int         `json:"news_max_items"`
	MediaMaxItems   int         `json:"media_max_items"`
	TargetScope     string      `json:"target_scope"`
	TargetRootHash  string      `json:"target_root_hash"`
	ProtectedCount  int         `json:"protected_count"`
	CreatedAt       time.Time   `json:"created_at"`
}

func recoveryApprovalPhrase(plan models.FeedRecoveryPlan) string {
	manifestPrefix := plan.ManifestHash
	if len(manifestPrefix) > 12 {
		manifestPrefix = manifestPrefix[:12]
	}
	if plan.Level == "purge_reseed" {
		phrase := "PURGE " + strings.ToUpper(plan.Lane) + " " + strconv.Itoa(plan.TargetCount) + " ITEMS " + strings.ToUpper(manifestPrefix)
		if plan.NoFullRollback {
			phrase += " NO FULL ROLLBACK"
		}
		return phrase
	}
	return "APPROVE FEED RECOVERY " + strings.ToUpper(manifestPrefix)
}

func normalizedRecoveryField(value string, allowed ...string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return ""
}
func recoveryHash(value interface{}) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

const recoveryTargetPageSize = 500

func collectRecoveryTargetIDs(db *gorm.DB, tenant, lane string, contentType models.ContentType) ([]uuid.UUID, int, error) {
	var ids []uuid.UUID
	protectedCount := 0
	var last uuid.UUID
	for {
		q := db.Model(&models.ContentItem{}).Where("tenant_id=? AND type=? AND status=?", tenant, contentType, models.ContentStatusReady)
		if contentType == models.ContentTypeVideo || contentType == models.ContentTypePodcast {
			q = q.Where("is_feed_unit=TRUE AND feed_visibility=?", feedVisibilityVisible)
		}
		if last != uuid.Nil {
			q = q.Where("public_id > ?", last)
		}
		var page []uuid.UUID
		if err := q.Order("public_id ASC").Limit(recoveryTargetPageSize).Pluck("public_id", &page).Error; err != nil {
			return nil, 0, err
		}
		if len(page) == 0 {
			break
		}
		protected, err := retentionProtectedContentIDs(db, tenant, page)
		if err != nil {
			return nil, 0, err
		}
		for _, id := range page {
			if protected[id] {
				protectedCount++
				continue
			}
			ids = append(ids, id)
		}
		last = page[len(page)-1]
		if len(page) < recoveryTargetPageSize {
			break
		}
	}
	return ids, protectedCount, nil
}

func recoveryTargetRootHash(manifest recoveryPurgeManifest) string {
	parts := make([]string, 0, len(manifest.NewsContentIDs)+len(manifest.MediaContentIDs))
	for _, id := range manifest.NewsContentIDs {
		parts = append(parts, "news_content|"+id.String())
	}
	for _, id := range manifest.MediaContentIDs {
		parts = append(parts, "media_content|"+id.String())
	}
	return recoveryHash(parts)
}

func buildRecoveryPurgeManifest(db *gorm.DB, tenant, lane, sourceChecksum string) (recoveryPurgeManifest, error) {
	manifest := recoveryPurgeManifest{Version: 1, TenantID: tenant, Lane: lane, LookbackHours: 72, NewsMaxItems: 500, MediaMaxItems: 30, TargetScope: "all_eligible_unprotected_content", CreatedAt: time.Now().UTC()}
	q := db.Model(&models.ContentSource{}).Where("tenant_id=? AND is_active=?", tenant, true)
	if lane == "news" {
		q = q.Where("category=?", "news")
	} else if lane == "media" {
		q = q.Where("category=?", "media")
	}
	var sources []models.ContentSource
	if err := q.Order("public_id ASC").Find(&sources).Error; err != nil {
		return manifest, err
	}
	for _, source := range sources {
		manifest.SourceIDs = append(manifest.SourceIDs, source.PublicID)
	}
	if len(manifest.SourceIDs) > 200 {
		return manifest, fmt.Errorf("purge manifest exceeds the 200-source recovery bound")
	}
	if lane == "news" || lane == "both" {
		ids, protectedCount, err := collectRecoveryTargetIDs(db, tenant, "news", models.ContentTypeNews)
		if err != nil {
			return manifest, err
		}
		manifest.NewsContentIDs = ids
		manifest.ProtectedCount += protectedCount
		if len(manifest.NewsContentIDs) > 0 {
			if err := db.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id IN ?", tenant, manifest.NewsContentIDs).Where("story_id IS NOT NULL").Distinct("story_id").Pluck("story_id", &manifest.NewsStoryIDs).Error; err != nil {
				return manifest, fmt.Errorf("recovery news story scope: %w", err)
			}
		}
	}
	if lane == "media" || lane == "both" {
		for _, contentType := range []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast} {
			ids, protectedCount, err := collectRecoveryTargetIDs(db, tenant, "media", contentType)
			if err != nil {
				return manifest, err
			}
			manifest.MediaContentIDs = append(manifest.MediaContentIDs, ids...)
			manifest.ProtectedCount += protectedCount
		}
	}
	manifest.TargetRootHash = recoveryTargetRootHash(manifest)
	_ = sourceChecksum // kept in the surrounding plan hash/evidence contract.
	return manifest, nil
}

func recoveryManifestIDs(manifest recoveryPurgeManifest, lane string) []uuid.UUID {
	if lane == "news" {
		return manifest.NewsContentIDs
	}
	if lane == "media" {
		return manifest.MediaContentIDs
	}
	return append(append([]uuid.UUID{}, manifest.NewsContentIDs...), manifest.MediaContentIDs...)
}

func recoverySourceProof(db *gorm.DB, tenant, lane string) (string, int, error) {
	q := db.Model(&models.ContentSource{}).Where("tenant_id=? AND is_active=?", tenant, true)
	if lane == "news" {
		q = q.Where("category=?", "news")
	} else if lane == "media" {
		q = q.Where("category=?", "media")
	}
	var sources []models.ContentSource
	if err := q.Select("public_id, updated_at, category, type").Find(&sources).Error; err != nil {
		return "", 0, err
	}
	parts := make([]string, 0, len(sources))
	for _, source := range sources {
		parts = append(parts, source.PublicID.String()+"|"+source.Category+"|"+string(source.Type)+"|"+source.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	sort.Strings(parts)
	return recoveryHash(parts), len(parts), nil
}

func buildRecoveryPlan(db *gorm.DB, tenant string, req feedRecoveryPlanRequest, actor string) (models.FeedRecoveryPlan, error) {
	lane := normalizedRecoveryField(req.Lane, "news", "media", "both")
	level := normalizedRecoveryField(req.Level, "repair", "rotate", "purge_reseed")
	mode := normalizedRecoveryField(req.CapacityMode, "safe_cutover", "low_space_reset")
	if lane == "" || level == "" || mode == "" {
		return models.FeedRecoveryPlan{}, gorm.ErrInvalidData
	}
	if level != "purge_reseed" && req.NoFullRollback {
		return models.FeedRecoveryPlan{}, gorm.ErrInvalidData
	}
	if level != "purge_reseed" && mode != "safe_cutover" {
		return models.FeedRecoveryPlan{}, gorm.ErrInvalidData
	}
	checksum, count, err := recoverySourceProof(db, tenant, lane)
	if err != nil {
		return models.FeedRecoveryPlan{}, err
	}
	evidence := map[string]interface{}{"source_checksum": checksum, "source_count": count, "execution_installed": true, "enabled_levels": []string{"repair", "rotate", "purge_reseed"}, "safe_note": "Repair and Safe Cutover change derived feed state; Purge & Reseed only touches the frozen content manifest and never sources/checkpoints."}
	manifest := map[string]interface{}{"tenant": tenant, "lane": lane, "level": level, "capacity_mode": mode, "source_checksum": checksum, "source_count": count}
	var purgeManifest recoveryPurgeManifest
	if level == "purge_reseed" {
		purgeManifest, err = buildRecoveryPurgeManifest(db, tenant, lane, checksum)
		if err != nil {
			return models.FeedRecoveryPlan{}, err
		}
		manifest["purge_manifest"] = purgeManifest
		evidence["purge_manifest"] = purgeManifest
	}
	manifestHash := recoveryHash(manifest)
	planHash := recoveryHash(map[string]interface{}{"manifest": manifest, "no_full_rollback": req.NoFullRollback})
	evidenceJSON, _ := json.Marshal(evidence)
	policyJSON, _ := json.Marshal(map[string]interface{}{"execution": "repair_rotate_and_exact_purge_reseed", "purge_reseed_caps": map[string]int{"news_lookback_hours": 72, "news_max_items": 500, "media_max_items": 30}, "purge_target_scope": "all_eligible_unprotected_content", "sources": "preserve_definitions_and_checkpoints"})
	targetCount := 0
	if level == "purge_reseed" {
		targetCount = len(purgeManifest.NewsContentIDs) + len(purgeManifest.MediaContentIDs)
	}
	purgeJSON, _ := json.Marshal(purgeManifest)
	var frozenAt *time.Time
	if level == "purge_reseed" {
		t := purgeManifest.CreatedAt
		frozenAt = &t
	}
	targetRootHash := ""
	targetScope := ""
	if level == "purge_reseed" {
		targetRootHash = purgeManifest.TargetRootHash
		targetScope = purgeManifest.TargetScope
	}
	targetSegments := 0
	if targetCount > 0 {
		targetSegments = (targetCount + recoveryTargetPageSize - 1) / recoveryTargetPageSize
	}
	return models.FeedRecoveryPlan{TenantID: tenant, Lane: lane, Level: level, CapacityMode: mode, State: "awaiting_approval", PlanHash: planHash, ManifestHash: manifestHash, TargetCount: targetCount, TargetRootHash: targetRootHash, TargetScope: targetScope, TargetSegments: targetSegments, TargetByteSize: int64(targetCount * 16), SourceChecksum: checksum, SourceCount: count, Evidence: datatypes.JSON(evidenceJSON), PolicySnapshot: datatypes.JSON(policyJSON), NoFullRollback: req.NoFullRollback, PurgeManifest: datatypes.JSON(purgeJSON), ManifestFrozenAt: frozenAt, ExpiresAt: time.Now().UTC().Add(feedRecoveryPlanTTL), CreatedBy: actor}, nil
}

func persistRecoveryPlanTargets(tx *gorm.DB, plan models.FeedRecoveryPlan, manifest recoveryPurgeManifest) error {
	if plan.Level != "purge_reseed" {
		return nil
	}
	rows := make([]models.FeedRecoveryPlanTarget, 0, len(manifest.NewsContentIDs)+len(manifest.MediaContentIDs))
	ordinal := int64(1)
	appendTargets := func(ids []uuid.UUID, lane, targetType string) {
		for _, id := range ids {
			rows = append(rows, models.FeedRecoveryPlanTarget{PlanID: plan.ID, TenantID: plan.TenantID, Lane: lane, TargetType: targetType, TargetID: id, Ordinal: ordinal, EvidenceHash: recoveryHash([]string{plan.ManifestHash, targetType, id.String()})})
			ordinal++
		}
	}
	if plan.Lane == "news" || plan.Lane == "both" {
		appendTargets(manifest.NewsContentIDs, "news", "news_content")
	}
	if plan.Lane == "media" || plan.Lane == "both" {
		appendTargets(manifest.MediaContentIDs, "media", "media_content")
	}
	if len(rows) == 0 {
		return nil
	}
	return tx.Create(&rows).Error
}

func ensureFeedGenerationFoundation(db *gorm.DB, tenant, requestedLane string) error {
	lanes := []string{requestedLane}
	if requestedLane == "both" {
		lanes = []string{"news", "media"}
	}
	for _, lane := range lanes {
		if err := db.Transaction(func(tx *gorm.DB) error {
			var head models.FeedGenerationHead
			if err := tx.Where("tenant_id=? AND lane=?", tenant, lane).First(&head).Error; err == nil {
				return nil
			} else if err != gorm.ErrRecordNotFound {
				return err
			}
			generation := models.FeedGeneration{TenantID: tenant, Lane: lane, State: "active", BuildWatermark: time.Now().UTC(), Verification: datatypes.JSON([]byte(`{"foundation":true}`))}
			if err := tx.Create(&generation).Error; err != nil {
				return err
			}
			head = models.FeedGenerationHead{TenantID: tenant, Lane: lane, ActiveGenerationID: &generation.PublicID, Generation: 1}
			if err := tx.Create(&head).Error; err != nil {
				return err
			}
			if lane == "news" {
				return tx.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'story', story_id FROM content_items WHERE tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL GROUP BY story_id ON CONFLICT DO NOTHING", generation.PublicID, tenant).Error
			}
			return tx.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'feed_unit', public_id FROM content_items WHERE tenant_id=? AND type IN ('VIDEO','PODCAST') AND status='READY' AND is_feed_unit=TRUE AND feed_visibility='visible' ON CONFLICT DO NOTHING", generation.PublicID, tenant).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

func CreateFeedRecoveryPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req feedRecoveryPlanRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid recovery plan"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	plan, err := buildRecoveryPlan(db, principal.TenantID, req, principal.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "lane, level, capacity_mode, and rollback acknowledgement are invalid"})
		return
	}
	if err := ensureFeedGenerationFoundation(db, principal.TenantID, plan.Lane); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to initialize feed generation boundary"})
		return
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&plan).Error; err != nil {
			return err
		}
		var manifest recoveryPurgeManifest
		if plan.Level == "purge_reseed" {
			var decodeErr error
			manifest, decodeErr = decodeRecoveryPurgeManifest(plan)
			if decodeErr != nil {
				return decodeErr
			}
			if err := persistRecoveryPlanTargets(tx, plan, manifest); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create recovery plan"})
		return
	}
	c.JSON(http.StatusCreated, plan)
}

func GetFeedRecoveryPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	var plan models.FeedRecoveryPlan
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&plan).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	c.JSON(http.StatusOK, plan)
}

func RefreshFeedRecoveryPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var old models.FeedRecoveryPlan
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&old).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	plan, err := buildRecoveryPlan(db, principal.TenantID, feedRecoveryPlanRequest{Lane: old.Lane, Level: old.Level, CapacityMode: old.CapacityMode, NoFullRollback: old.NoFullRollback}, principal.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unable to refresh preflight"})
		return
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&plan).Error; err != nil {
			return err
		}
		if plan.Level == "purge_reseed" {
			manifest, decodeErr := decodeRecoveryPurgeManifest(plan)
			if decodeErr != nil {
				return decodeErr
			}
			return persistRecoveryPlanTargets(tx, plan, manifest)
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to refresh preflight"})
		return
	}
	c.JSON(http.StatusCreated, plan)
}

func recoveryRunExpectedEmpty(plan models.FeedRecoveryPlan) bool {
	// A low-space reset intentionally removes the old selected lane before
	// reseeding it. A safe cutover keeps the current generation serving, and a
	// zero-target purge is not evidence that production is empty.
	return plan.Level == "purge_reseed" && plan.CapacityMode == "low_space_reset" && plan.TargetCount > 0
}

func ApproveFeedRecoveryPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok || !principal.HasRole("admin") {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin role required"})
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	var req struct {
		Phrase string `json:"phrase"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "approval details required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var plan models.FeedRecoveryPlan
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&plan).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	if !plan.ExpiresAt.After(time.Now().UTC()) || plan.State != "awaiting_approval" {
		c.JSON(http.StatusConflict, gin.H{"error": "plan is no longer approvable"})
		return
	}
	expectedPhrase := recoveryApprovalPhrase(plan)
	if strings.TrimSpace(req.Phrase) != expectedPhrase {
		c.JSON(http.StatusBadRequest, gin.H{"error": "confirmation phrase does not match plan"})
		return
	}
	now := time.Now().UTC()
	notBefore := now.Add(time.Minute)
	phraseHash := recoveryHash(req.Phrase)
	// Admin JWT + feed:manage authorization and the manifest-bound phrase are
	// sufficient approval proof. Keep the existing unique audit column populated
	// with a server-generated identifier; it is not a password credential.
	approval := models.FeedRecoveryApproval{PlanID: plan.ID, TenantID: plan.TenantID, Actor: principal.Email, PlanHash: plan.PlanHash, ManifestHash: plan.ManifestHash, TargetCount: plan.TargetCount, PhraseProofHash: phraseHash, ReauthJTI: uuid.New().String(), NoFullRollback: plan.NoFullRollback, ApprovedAt: now, ConsumedAt: &now}
	// Low-Space Purge & Reseed intentionally exposes an expected-empty state
	// while the approved targets are being removed and reseeded. A zero-target
	// plan is not evidence of an empty production feed.
	expectedEmpty := recoveryRunExpectedEmpty(plan)
	run := models.FeedRecoveryRun{PlanID: plan.ID, TenantID: plan.TenantID, Lane: plan.Lane, CorrelationID: uuid.New(), Phase: "cancel_window", NotBefore: &notBefore, CancelDeadline: &notBefore, DestructiveManifest: plan.PurgeManifest, ExpectedEmpty: expectedEmpty}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&approval).Error; err != nil {
			return err
		}
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		approvalEvidence := map[string]interface{}{"authorization": "admin_jwt_feed_manage", "fresh_reauth": false, "cancel_window_seconds": 60, "manifest_hash": plan.ManifestHash, "target_count": plan.TargetCount}
		if plan.Level == "purge_reseed" {
			approvalEvidence["destructive"] = true
			approvalEvidence["no_full_rollback"] = plan.NoFullRollback
		}
		approvalRaw, _ := json.Marshal(approvalEvidence)
		if err := tx.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "approval", State: "succeeded", IdempotencyKey: "approval:" + plan.PlanHash, Evidence: datatypes.JSON(approvalRaw)}).Error; err != nil {
			return err
		}
		return tx.Model(&plan).Update("state", "approved").Error
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "approval proof was already consumed or approval could not be saved"})
		return
	}
	run.PlanPublicID = plan.PublicID
	c.JSON(http.StatusOK, gin.H{"plan": plan.PublicID, "run": run, "cancel_deadline": notBefore})
}

func ListFeedRecoveryRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var runs []models.FeedRecoveryRun
	if err := db.Where("tenant_id=?", principal.TenantID).Order("created_at DESC").Limit(100).Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list runs"})
		return
	}
	attachRecoveryPlanPublicIDs(db, principal.TenantID, runs)
	c.JSON(http.StatusOK, gin.H{"data": runs})
}

func attachRecoveryPlanPublicIDs(db *gorm.DB, tenant string, runs []models.FeedRecoveryRun) {
	if len(runs) == 0 {
		return
	}
	ids := make([]uint, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.PlanID)
	}
	var plans []models.FeedRecoveryPlan
	if db.Select("id", "public_id").Where("tenant_id=? AND id IN ?", tenant, ids).Find(&plans).Error != nil {
		return
	}
	byID := make(map[uint]uuid.UUID, len(plans))
	for _, plan := range plans {
		byID[plan.ID] = plan.PublicID
	}
	for i := range runs {
		if publicID, ok := byID[runs[i].PlanID]; ok {
			runs[i].PlanPublicID = publicID
		}
	}
}

func GetFeedRecoveryRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedRecoveryRun
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	attachRecoveryPlanPublicIDs(db, principal.TenantID, []models.FeedRecoveryRun{run})
	c.JSON(http.StatusOK, run)
}

func ListFeedRecoveryActions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedRecoveryRun
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	var actions []models.FeedRecoveryAction
	if err := db.Where("run_id=?", run.ID).Order("created_at ASC").Find(&actions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list actions"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": actions})
}

const recoveryClaimTTL = 2 * time.Minute
const recoveryLaneLeaseTTL = 10 * time.Minute
const recoveryRollbackTTL = 24 * time.Hour

// Feed Recovery only invokes tools registered here. Each tool is bounded to a
// derived feed/cache artifact; none is allowed to delete canonical content or
// mutate source checkpoints.
type feedRecoveryRepairTool struct {
	Name        string
	Description string
}

var registeredFeedRecoveryRepairTools = []feedRecoveryRepairTool{
	{Name: "feed_generation_membership_reconcile", Description: "attach current READY feed members to the active generation"},
	{Name: "news_snapshot_rebuild", Description: "invalidate and rebuild today/week/month News snapshots"},
}

func recoveryLanes(lane string) []string {
	if lane == "both" {
		return []string{"news", "media"}
	}
	return []string{lane}
}

func acquireRecoveryLaneLeases(tx *gorm.DB, run models.FeedRecoveryRun, now time.Time) (string, error) {
	lanes := recoveryLanes(run.Lane)
	// Both always acquires in News → Media order. This fixed order prevents a
	// News-only and Media-only run from deadlocking each other.
	if len(lanes) == 2 {
		lanes = []string{"news", "media"}
	}
	token := uuid.New()
	expires := now.Add(recoveryLaneLeaseTTL)
	for _, lane := range lanes {
		var lease models.FeedRecoveryLaneLease
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", run.TenantID, lane).First(&lease).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			lease = models.FeedRecoveryLaneLease{TenantID: run.TenantID, Lane: lane, RunID: run.ID, FencingToken: token, AcquiredAt: now, HeartbeatAt: now, ExpiresAt: expires}
			if err := tx.Create(&lease).Error; err != nil {
				return "", err
			}
			continue
		}
		if err != nil {
			return "", err
		}
		if lease.RunID != run.ID && lease.ExpiresAt.After(now) {
			return "", fmt.Errorf("recovery lane %s is owned by another active run", lane)
		}
		if lease.RunID == run.ID {
			// The same resumable run renews its fencing token. It never creates a
			// second token while moving from probe one to probe two.
			token = lease.FencingToken
			if err := tx.Model(&lease).Updates(map[string]interface{}{"heartbeat_at": now, "expires_at": expires}).Error; err != nil {
				return "", err
			}
			continue
		}
		if err := tx.Model(&lease).Updates(map[string]interface{}{"run_id": run.ID, "fencing_token": token, "acquired_at": now, "heartbeat_at": now, "expires_at": expires}).Error; err != nil {
			return "", err
		}
	}
	return token.String(), nil
}

func releaseRecoveryLaneLeases(db *gorm.DB, run models.FeedRecoveryRun) error {
	if strings.TrimSpace(run.LaneLease) == "" {
		return nil
	}
	result := db.Where("tenant_id=? AND run_id=? AND fencing_token=?", run.TenantID, run.ID, run.LaneLease).Delete(&models.FeedRecoveryLaneLease{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(recoveryLanes(run.Lane))) {
		return errors.New("recovery lane release lost its fencing lease")
	}
	return nil
}

func releaseRecoveryLaneLeasesOrRespond(c *gin.Context, db *gorm.DB, run models.FeedRecoveryRun) bool {
	if err := releaseRecoveryLaneLeases(db, run); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery lane release could not be verified"})
		return false
	}
	return true
}

func requireRecoveryLaneLease(db *gorm.DB, run models.FeedRecoveryRun) error {
	if strings.TrimSpace(run.LaneLease) == "" {
		return errors.New("recovery fencing token is missing")
	}
	var count int64
	if err := db.Model(&models.FeedRecoveryLaneLease{}).Where("tenant_id=? AND run_id=? AND fencing_token=? AND expires_at > ?", run.TenantID, run.ID, run.LaneLease, time.Now().UTC()).Count(&count).Error; err != nil {
		return err
	}
	if count != int64(len(recoveryLanes(run.Lane))) {
		return errors.New("recovery lane lease is stale or missing")
	}
	return nil
}

// updateRecoveryRunWithLease makes every execution-state mutation a fencing
// operation. A worker that lost its lease can no longer advance, clear, or
// restore a run after another worker took over.
func updateRecoveryRunWithLease(db *gorm.DB, run models.FeedRecoveryRun, updates map[string]interface{}) error {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return err
	}
	result := db.Model(&models.FeedRecoveryRun{}).Where("id=? AND tenant_id=? AND lane_lease=?", run.ID, run.TenantID, run.LaneLease).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("recovery run fencing token no longer owns the run")
	}
	return nil
}

func setRecoveryAvailabilityForRun(db *gorm.DB, run models.FeedRecoveryRun, state string) error {
	if strings.TrimSpace(run.LaneLease) == "" {
		return errors.New("recovery lane lease is missing")
	}
	token, err := uuid.Parse(run.LaneLease)
	if err != nil {
		return fmt.Errorf("recovery lane lease is invalid: %w", err)
	}
	for _, lane := range recoveryLanes(run.Lane) {
		var runID *uint
		if state != "normal" {
			runID = &run.ID
		}
		if err := setRecoveryAvailability(db, run.TenantID, lane, state, runID, &token); err != nil {
			return fmt.Errorf("%s availability: %w", lane, err)
		}
	}
	return nil
}

func reconcileRecoveryGenerationMemberships(db *gorm.DB, tenant, lane string) error {
	if err := ensureFeedGenerationFoundation(db, tenant, lane); err != nil {
		return err
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", tenant, lane).First(&head).Error; err != nil {
		return err
	}
	if head.ActiveGenerationID == nil {
		return fmt.Errorf("active generation is missing")
	}
	if lane == "news" {
		return db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'story', story_id FROM content_items WHERE tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL GROUP BY story_id ON CONFLICT DO NOTHING", *head.ActiveGenerationID, tenant).Error
	}
	return db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'feed_unit', public_id FROM content_items WHERE tenant_id=? AND type IN ('VIDEO','PODCAST') AND status='READY' AND is_feed_unit=TRUE AND feed_visibility='visible' ON CONFLICT DO NOTHING", *head.ActiveGenerationID, tenant).Error
}

func runRegisteredRecoveryRepairTools(db *gorm.DB, run models.FeedRecoveryRun) error {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return err
	}
	if len(registeredFeedRecoveryRepairTools) == 0 {
		return fmt.Errorf("no recovery repair tools are registered")
	}
	for _, lane := range recoveryLanes(run.Lane) {
		if err := reconcileRecoveryGenerationMemberships(db, run.TenantID, lane); err != nil {
			return fmt.Errorf("%s: %w", registeredFeedRecoveryRepairTools[0].Name, err)
		}
		if err := db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: registeredFeedRecoveryRepairTools[0].Name, State: "succeeded", IdempotencyKey: "repair-memberships:" + lane, Evidence: datatypes.JSON([]byte(fmt.Sprintf(`{"lane":"%s","non_destructive":true}`, lane)))}).Error; err != nil {
			return fmt.Errorf("record membership repair ledger: %w", err)
		}
	}
	if run.Lane == "news" || run.Lane == "both" {
		markNewsSnapshotDirty(db, run.TenantID)
		for _, window := range []string{"today", "week", "month"} {
			if _, err := buildNewsSnapshot(db, run.TenantID, window); err != nil {
				return fmt.Errorf("%s (%s): %w", registeredFeedRecoveryRepairTools[1].Name, window, err)
			}
		}
		if err := db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: registeredFeedRecoveryRepairTools[1].Name, State: "succeeded", IdempotencyKey: "repair-snapshots:" + run.PublicID.String(), Evidence: datatypes.JSON([]byte(`{"non_destructive":true,"windows":["today","week","month"]}`))}).Error; err != nil {
			return fmt.Errorf("record snapshot repair ledger: %w", err)
		}
	}
	return nil
}

func setRecoveryAvailability(db *gorm.DB, tenant, lane, state string, runID *uint, fencingToken *uuid.UUID) error {
	if fencingToken == nil {
		return errors.New("availability fencing token is missing")
	}
	message := "feed_ready"
	var retry *int
	if state == "refreshing" {
		message = "feed_refreshing"
		value := 30
		retry = &value
	}
	if state == "partial" {
		message = "feed_partial"
		value := 60
		retry = &value
	}
	if state == "expected_empty" {
		message = "feed_recovery_empty"
		value := 60
		retry = &value
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var leaseCount int64
		if err := tx.Model(&models.FeedRecoveryLaneLease{}).Where("tenant_id=? AND lane=? AND fencing_token=? AND expires_at > ?", tenant, lane, fencingToken, time.Now().UTC()).Count(&leaseCount).Error; err != nil || leaseCount != 1 {
			return fmt.Errorf("availability fencing lease is stale")
		}
		var current models.FeedAvailabilityState
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", tenant, lane).First(&current).Error
		updates := map[string]interface{}{"state": state, "recovery_run_id": runID, "message_key": message, "retry_after_seconds": retry, "updated_at": time.Now().UTC()}
		if state == "normal" {
			updates["fencing_token"] = nil
		} else {
			updates["fencing_token"] = fencingToken
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			rowToken := fencingToken
			if state == "normal" {
				rowToken = nil
			}
			row := models.FeedAvailabilityState{TenantID: tenant, Lane: lane, State: state, RecoveryRunID: runID, FencingToken: rowToken, MessageKey: message, RetryAfterSeconds: retry, UpdatedAt: time.Now().UTC()}
			return tx.Create(&row).Error
		}
		if err != nil {
			return err
		}
		return tx.Model(&current).Updates(updates).Error
	})
}

func claimRecoveryRun(db *gorm.DB, tenant string, publicID uuid.UUID) (models.FeedRecoveryRun, error) {
	var run models.FeedRecoveryRun
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", tenant, publicID).First(&run).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		if run.Phase == "cancel_window" {
			if run.NotBefore != nil && run.NotBefore.After(now) {
				return fmt.Errorf("cancel window remains open until %s", run.NotBefore.Format(time.RFC3339))
			}
		} else if run.Phase == "verification_wait" {
			if run.VerificationDueAt != nil && run.VerificationDueAt.After(now) {
				return fmt.Errorf("second verification is not due until %s", run.VerificationDueAt.Format(time.RFC3339))
			}
		} else if run.Phase == "partial" || run.Phase == "failed" {
			// A failed/partial run is resumable only through the same persisted
			// phase machine; it never reopens approval or widens its plan.
			run.Phase = "executing"
		} else if run.Phase != "executing" && run.Phase != "reseeding" && run.Phase != "purging_news" && run.Phase != "reseeding_news" && run.Phase != "purging_media" && run.Phase != "reseeding_media" {
			return fmt.Errorf("run is not resumable from phase %s", run.Phase)
		}
		if run.ClaimExpiresAt != nil && run.ClaimExpiresAt.After(now) {
			return fmt.Errorf("recovery run is already claimed")
		}
		laneToken, err := acquireRecoveryLaneLeases(tx, run, now)
		if err != nil {
			return err
		}
		token := uuid.New()
		expiry := now.Add(recoveryClaimTTL)
		updates := map[string]interface{}{"claim_token": token, "claim_expires_at": expiry, "heartbeat_at": now, "lane_lease": laneToken, "updated_at": now}
		if run.Phase == "cancel_window" {
			updates["phase"] = "executing"
			run.Phase = "executing"
		}
		if err := tx.Model(&run).Updates(updates).Error; err != nil {
			return err
		}
		run.ClaimToken, run.ClaimExpiresAt, run.HeartbeatAt, run.LaneLease = &token, &expiry, &now, laneToken
		return nil
	})
	return run, err
}

func buildRecoveryCandidate(db *gorm.DB, run models.FeedRecoveryRun) error {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", run.TenantID, run.Lane).First(&head).Error; err != nil {
			return err
		}
		if head.ActiveGenerationID == nil {
			return fmt.Errorf("active generation is missing")
		}
		if head.CandidateGenerationID != nil {
			run.CandidateGenerationID = head.CandidateGenerationID
			run.ActiveGenerationID = head.ActiveGenerationID
			return nil
		}
		generation := models.FeedGeneration{TenantID: run.TenantID, Lane: run.Lane, State: "candidate", PreviousGenerationID: head.ActiveGenerationID, BuildWatermark: time.Now().UTC(), Verification: datatypes.JSON([]byte(`{"source":"feed_recovery","complete":false}`))}
		if err := tx.Create(&generation).Error; err != nil {
			return err
		}
		if err := tx.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, member_type, member_id FROM feed_generation_memberships WHERE generation_id=? ON CONFLICT DO NOTHING", generation.PublicID, *head.ActiveGenerationID).Error; err != nil {
			return err
		}
		if err := tx.Model(&head).Updates(map[string]interface{}{"candidate_generation_id": generation.PublicID, "updated_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
		run.ActiveGenerationID, run.CandidateGenerationID = head.ActiveGenerationID, &generation.PublicID
		result := tx.Model(&models.FeedRecoveryRun{}).Where("id=? AND tenant_id=? AND lane_lease=?", run.ID, run.TenantID, run.LaneLease).Updates(map[string]interface{}{"active_generation_id": head.ActiveGenerationID, "candidate_generation_id": generation.PublicID, "phase": "reseeding", "heartbeat_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("recovery candidate mutation lost its fencing lease")
		}
		return nil
	})
}

func markRecoveryCandidateCaughtUp(db *gorm.DB, run models.FeedRecoveryRun) error {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return err
	}
	if run.CandidateGenerationID == nil {
		return fmt.Errorf("candidate generation is missing")
	}
	now := time.Now().UTC()
	var generation models.FeedGeneration
	if err := db.Where("public_id=? AND tenant_id=? AND lane=?", *run.CandidateGenerationID, run.TenantID, run.Lane).First(&generation).Error; err != nil {
		return err
	}
	// Reconcile anything that became READY after the candidate was seeded. The
	// normal ingest hook dual-writes this path, but the bounded catch-up makes a
	// crashed worker/retry safe and records the later watermark explicitly.
	if run.Lane == "news" {
		if err := db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'story', story_id FROM content_items WHERE tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL AND updated_at >= ? GROUP BY story_id ON CONFLICT DO NOTHING", *run.CandidateGenerationID, run.TenantID, generation.BuildWatermark).Error; err != nil {
			return err
		}
	} else {
		if err := db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'feed_unit', public_id FROM content_items WHERE tenant_id=? AND type IN ('VIDEO','PODCAST') AND status='READY' AND is_feed_unit=TRUE AND feed_visibility='visible' AND updated_at >= ? ON CONFLICT DO NOTHING", *run.CandidateGenerationID, run.TenantID, generation.BuildWatermark).Error; err != nil {
			return err
		}
	}
	return db.Model(&generation).Updates(map[string]interface{}{"state": "candidate", "caught_up_at": now, "build_watermark": now, "verification": datatypes.JSON([]byte(`{"caught_up":true,"dual_write_reconciled":true}`)), "updated_at": now}).Error
}

type recoveryInventoryProof struct {
	Lane                     string  `json:"lane"`
	NewsSlides               int64   `json:"news_slides"`
	NewsPageSlides           int64   `json:"news_page_slides"`
	NewsSources              int64   `json:"news_sources"`
	NewsDominantPct          float64 `json:"news_dominant_source_pct"`
	MediaUnits               int64   `json:"media_units"`
	MediaPageUnits           int64   `json:"media_page_units"`
	MediaSources             int64   `json:"media_sources"`
	MediaFamilies            int64   `json:"media_families"`
	PlaybackSamples          int64   `json:"playback_samples"`
	SuccessfulPlaybackProbes int64   `json:"successful_playback_probes"`
	Eligible                 bool    `json:"eligible"`
}

func feedIntegrityPageCount(db *gorm.DB, run models.FeedIntegrityRun, checkKey, feed, variant string) (int64, error) {
	var count int64
	if err := db.Model(&models.FeedIntegrityFinding{}).
		Where("run_id=? AND check_key=? AND feed=? AND variant=? AND status=?", run.ID, checkKey, feed, variant, "ok").
		Select("COALESCE(SUM(candidate_count), 0)").Scan(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func recoveryInventoryProofForLane(db *gorm.DB, tenant, lane string) (recoveryInventoryProof, error) {
	proof := recoveryInventoryProof{Lane: lane}
	if lane == "news" {
		if err := db.Model(&models.Story{}).Where("tenant_id = ? AND last_member_at IS NOT NULL AND last_member_at > ? AND EXISTS (SELECT 1 FROM content_items ci WHERE ci.tenant_id = stories.tenant_id AND ci.story_id = stories.public_id AND ci.type = ? AND ci.status = ?)", tenant, time.Now().Add(-31*24*time.Hour), models.ContentTypeNews, models.ContentStatusReady).Count(&proof.NewsSlides).Error; err != nil {
			return proof, err
		}
		var sourceCount int64
		if err := db.Model(&models.ContentItem{}).Where("tenant_id = ? AND type = ? AND status = ? AND story_id IN (SELECT public_id FROM stories WHERE tenant_id = ? AND last_member_at IS NOT NULL AND last_member_at > ?)", tenant, models.ContentTypeNews, models.ContentStatusReady, tenant, time.Now().Add(-31*24*time.Hour)).Select("COUNT(DISTINCT COALESCE(NULLIF(BTRIM(source_name), ''), source::text))").Scan(&sourceCount).Error; err != nil {
			return proof, err
		}
		proof.NewsSources = sourceCount
		var dominant int64
		if err := db.Raw("SELECT COALESCE(MAX(n), 0) FROM (SELECT COUNT(*) n FROM content_items WHERE tenant_id=? AND type='NEWS' AND status='READY' AND story_id IN (SELECT public_id FROM stories WHERE tenant_id=? AND last_member_at IS NOT NULL AND last_member_at > ?) GROUP BY COALESCE(NULLIF(BTRIM(source_name), ''), source::text)) q", tenant, tenant, time.Now().Add(-31*24*time.Hour)).Scan(&dominant).Error; err != nil {
			return proof, err
		}
		var total int64
		if err := db.Model(&models.ContentItem{}).Where("tenant_id=? AND type=? AND status=? AND story_id IN (SELECT public_id FROM stories WHERE tenant_id=? AND last_member_at IS NOT NULL AND last_member_at > ?)", tenant, models.ContentTypeNews, models.ContentStatusReady, tenant, time.Now().Add(-31*24*time.Hour)).Count(&total).Error; err != nil {
			return proof, err
		}
		if total > 0 {
			proof.NewsDominantPct = float64(dominant) / float64(total)
		}
		proof.Eligible = proof.NewsSlides >= 10 && proof.NewsSources >= 3 && proof.NewsDominantPct <= 0.50
		return proof, nil
	}
	if lane == "media" {
		base := db.Model(&models.ContentItem{}).Where("tenant_id=? AND type IN ? AND status=? AND is_feed_unit=TRUE AND feed_visibility=?", tenant, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, models.ContentStatusReady, feedVisibilityVisible)
		if err := base.Count(&proof.MediaUnits).Error; err != nil {
			return proof, err
		}
		if err := base.Select("COUNT(DISTINCT COALESCE(parent_content_item_id, public_id))").Scan(&proof.MediaFamilies).Error; err != nil {
			return proof, err
		}
		if err := base.Select("COUNT(DISTINCT COALESCE(NULLIF(BTRIM(source_name), ''), source::text))").Scan(&proof.MediaSources).Error; err != nil {
			return proof, err
		}
		if err := base.Where("NULLIF(BTRIM(COALESCE(playback_url, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(media_url, '')), '') IS NOT NULL").Limit(10).Count(&proof.PlaybackSamples).Error; err != nil {
			return proof, err
		}
		proof.Eligible = proof.MediaUnits >= 20 && proof.MediaSources >= 3 && proof.MediaFamilies >= 10 && proof.PlaybackSamples >= 10
		return proof, nil
	}
	return proof, fmt.Errorf("unknown recovery lane %s", lane)
}

func systemHealthRunHealthy(run models.SystemAutopilotRun) bool {
	if run.Status != models.SystemAutopilotRunStatusCompleted {
		return false
	}
	var wrapper struct {
		RunSnapshot struct {
			Overall string `json:"overall"`
		} `json:"run_snapshot"`
	}
	if json.Unmarshal(run.ProbeResults, &wrapper) != nil {
		return false
	}
	return wrapper.RunSnapshot.Overall == "healthy"
}

func feedIntegrityLatencyClean(db *gorm.DB, run models.FeedIntegrityRun) bool {
	var findings []models.FeedIntegrityFinding
	if db.Where("run_id = ?", run.ID).Find(&findings).Error != nil {
		return false
	}
	for _, finding := range findings {
		if strings.Contains(finding.CheckKey, "latency") && finding.Status != "ok" {
			return false
		}
	}
	return true
}

func feedIntegritySuccessfulPlaybackProbes(db *gorm.DB, run models.FeedIntegrityRun) (int64, error) {
	var count int64
	if err := db.Model(&models.FeedIntegrityFinding{}).Where("run_id=? AND check_key=? AND feed=? AND status=?", run.ID, "probe_url_success", "pods", "ok").Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// recoveryCandidateMembershipProof verifies the private generation that will
// be cut over. Recovery verification must not rely only on the currently
// active public feed: a candidate can be populated correctly while the active
// generation remains healthy (or vice versa).
func recoveryCandidateMembershipProof(db *gorm.DB, run models.FeedRecoveryRun, lane string) (bool, error) {
	if run.CandidateGenerationID == nil {
		return false, errors.New("candidate generation is missing")
	}
	var generation models.FeedGeneration
	if err := db.Where("public_id=? AND tenant_id=? AND lane=?", *run.CandidateGenerationID, run.TenantID, lane).First(&generation).Error; err != nil {
		return false, err
	}
	if generation.State != "candidate" || generation.CaughtUpAt == nil {
		return false, fmt.Errorf("candidate generation is not caught up")
	}
	memberType := "story"
	if lane == "media" {
		memberType = "feed_unit"
	}
	var count int64
	if err := db.Model(&models.FeedGenerationMembership{}).Where("generation_id=? AND member_type=?", *run.CandidateGenerationID, memberType).Count(&count).Error; err != nil {
		return false, err
	}
	if lane == "news" {
		if count < 10 {
			return false, fmt.Errorf("candidate has only %d news stories", count)
		}
		var invalid int64
		if err := db.Raw(`SELECT COUNT(*) FROM feed_generation_memberships m
			LEFT JOIN stories s ON s.public_id=m.member_id AND s.tenant_id=?
			WHERE m.generation_id=? AND m.member_type='story' AND (s.public_id IS NULL OR s.last_member_at IS NULL)`, run.TenantID, *run.CandidateGenerationID).Scan(&invalid).Error; err != nil {
			return false, err
		}
		return invalid == 0, nil
	}
	if count < 20 {
		return false, fmt.Errorf("candidate has only %d media units", count)
	}
	var invalid int64
	if err := db.Raw(`SELECT COUNT(*) FROM feed_generation_memberships m
		LEFT JOIN content_items c ON c.public_id=m.member_id AND c.tenant_id=?
		WHERE m.generation_id=? AND m.member_type='feed_unit' AND (c.public_id IS NULL OR c.status <> 'READY' OR c.is_feed_unit IS NOT TRUE OR c.feed_visibility <> 'visible' OR (NULLIF(BTRIM(COALESCE(c.playback_url,'')), '') IS NULL AND NULLIF(BTRIM(COALESCE(c.media_url,'')), '') IS NULL))`, run.TenantID, *run.CandidateGenerationID).Scan(&invalid).Error; err != nil {
		return false, err
	}
	return invalid == 0, nil
}

func runRecoveryVerification(db *gorm.DB, run models.FeedRecoveryRun, pass int) bool {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return false
	}
	fi, fiErr := runFeedIntegrity(db, run.TenantID, feedIntegrityRunOptions{Trigger: "feed_recovery", CreatedBy: "feed_recovery", Tier: models.FeedIntegrityTierDeep, CorrelationID: &run.CorrelationID, TriggerRef: run.PublicID.String()})
	sh, shActions, shErr := runSystemHealthAutopilot(db, systemAutopilotRunOptions{Trigger: "feed_recovery", CreatedBy: "feed_recovery", CorrelationID: &run.CorrelationID, TriggerRef: run.PublicID.String()})
	proofs := make([]recoveryInventoryProof, 0, len(recoveryLanes(run.Lane)))
	proofOK := true
	candidateOK := true
	if run.CandidateGenerationID != nil {
		for _, lane := range recoveryLanes(run.Lane) {
			ok, err := recoveryCandidateMembershipProof(db, run, lane)
			candidateOK = candidateOK && err == nil && ok
		}
	} else if run.ActiveGenerationID != nil {
		candidateOK = false
	}
	for _, lane := range recoveryLanes(run.Lane) {
		proof, err := recoveryInventoryProofForLane(db, run.TenantID, lane)
		if err != nil {
			proofOK = false
		} else {
			if fiErr == nil {
				if lane == "news" {
					pageSlides, pageErr := feedIntegrityPageCount(db, fi, "edge_news_page_success", "news", "window:today")
					if pageErr != nil {
						proofOK = false
					} else {
						proof.NewsPageSlides = pageSlides
						proof.Eligible = proof.Eligible && pageSlides >= 10
					}
				} else if lane == "media" {
					pageUnits, pageErr := feedIntegrityPageCount(db, fi, "edge_pods_page_success", "pods", "default")
					if pageErr != nil {
						proofOK = false
					} else {
						proof.MediaPageUnits = pageUnits
						proof.Eligible = proof.Eligible && pageUnits >= 20
					}
				}
			}
			if lane == "media" && fiErr == nil {
				probes, probeErr := feedIntegritySuccessfulPlaybackProbes(db, fi)
				if probeErr != nil {
					proofOK = false
				} else {
					proof.SuccessfulPlaybackProbes = probes
					proof.Eligible = proof.Eligible && probes >= 10
				}
			}
			proofs = append(proofs, proof)
			proofOK = proofOK && proof.Eligible
		}
	}
	clean := fiErr == nil && shErr == nil && fi.Status == models.FeedIntegrityRunCompleted && fi.Headline == "all_clear" && feedIntegrityLatencyClean(db, fi) && systemHealthRunHealthy(sh) && proofOK && candidateOK
	state := "failed"
	if clean {
		state = "succeeded"
	}
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return false
	}
	var attempts int64
	if err := db.Model(&models.FeedRecoveryAction{}).Where("run_id=? AND action_type=?", run.ID, fmt.Sprintf("verification_probe_%d", pass)).Count(&attempts).Error; err != nil {
		return false
	}
	gateMatrix := map[string]interface{}{
		"feed_integrity_completed":      fiErr == nil && fi.Status == models.FeedIntegrityRunCompleted,
		"feed_integrity_all_clear":      fiErr == nil && fi.Headline == "all_clear",
		"feed_integrity_latency_clean":  fiErr == nil && feedIntegrityLatencyClean(db, fi),
		"system_health_healthy":         shErr == nil && systemHealthRunHealthy(sh),
		"candidate_membership_verified": candidateOK,
		"lanes":                         proofs,
	}
	evidence, _ := json.Marshal(map[string]interface{}{"feed_integrity_run": fi.PublicID, "feed_integrity_headline": fi.Headline, "system_health_run": sh.PublicID, "system_health_overall_healthy": systemHealthRunHealthy(sh), "system_health_actions": len(shActions), "feed_integrity_error": fiErr != nil, "system_health_error": shErr != nil, "latency_clean": feedIntegrityLatencyClean(db, fi), "gate_matrix": gateMatrix, "inventory": proofs})
	if err := db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: fmt.Sprintf("verification_probe_%d", pass), State: state, IdempotencyKey: fmt.Sprintf("verify:%d:%d", pass, attempts+1), Evidence: datatypes.JSON(evidence)}).Error; err != nil {
		return false
	}
	return clean
}

func decodeRecoveryPurgeManifest(plan models.FeedRecoveryPlan) (recoveryPurgeManifest, error) {
	var manifest recoveryPurgeManifest
	if len(plan.PurgeManifest) == 0 || json.Unmarshal(plan.PurgeManifest, &manifest) != nil {
		return manifest, fmt.Errorf("purge manifest is unreadable")
	}
	if manifest.TenantID != plan.TenantID || manifest.Lane != plan.Lane || manifest.Version != 1 {
		return manifest, fmt.Errorf("purge manifest scope is invalid")
	}
	outer := map[string]interface{}{"tenant": plan.TenantID, "lane": plan.Lane, "level": plan.Level, "capacity_mode": plan.CapacityMode, "source_checksum": plan.SourceChecksum, "source_count": plan.SourceCount, "purge_manifest": manifest}
	if recoveryHash(outer) != plan.ManifestHash {
		return manifest, fmt.Errorf("purge manifest hash does not match approval")
	}
	return manifest, nil
}

func verifyRecoveryPlanTargets(db *gorm.DB, plan models.FeedRecoveryPlan, manifest recoveryPurgeManifest) error {
	if plan.Level != "purge_reseed" {
		return nil
	}
	var rows []models.FeedRecoveryPlanTarget
	if err := db.Where("plan_id=? AND tenant_id=?", plan.ID, plan.TenantID).Order("ordinal ASC").Find(&rows).Error; err != nil {
		return err
	}
	if len(rows) != plan.TargetCount {
		return fmt.Errorf("exact recovery target manifest is incomplete: got %d want %d", len(rows), plan.TargetCount)
	}
	expected := make(map[string]struct{}, plan.TargetCount)
	for _, id := range manifest.NewsContentIDs {
		expected["news_content|"+id.String()] = struct{}{}
	}
	for _, id := range manifest.MediaContentIDs {
		expected["media_content|"+id.String()] = struct{}{}
	}
	if len(expected) != plan.TargetCount {
		return fmt.Errorf("purge manifest target count is inconsistent")
	}
	for _, row := range rows {
		key := row.TargetType + "|" + row.TargetID.String()
		if _, ok := expected[key]; !ok || row.TenantID != plan.TenantID {
			return fmt.Errorf("recovery target row is outside the frozen manifest")
		}
	}
	if plan.TargetRootHash == "" || manifest.TargetRootHash == "" || manifest.TargetRootHash != plan.TargetRootHash || recoveryTargetRootHash(manifest) != plan.TargetRootHash {
		return fmt.Errorf("recovery target root hash is invalid")
	}
	return nil
}

func hydrateRecoveryManifestTargets(db *gorm.DB, plan models.FeedRecoveryPlan, manifest recoveryPurgeManifest) (recoveryPurgeManifest, error) {
	var rows []models.FeedRecoveryPlanTarget
	if err := db.Where("plan_id=? AND tenant_id=?", plan.ID, plan.TenantID).Order("ordinal ASC").Find(&rows).Error; err != nil {
		return manifest, err
	}
	manifest.NewsContentIDs = nil
	manifest.MediaContentIDs = nil
	for _, row := range rows {
		switch row.TargetType {
		case "news_content":
			manifest.NewsContentIDs = append(manifest.NewsContentIDs, row.TargetID)
		case "media_content":
			manifest.MediaContentIDs = append(manifest.MediaContentIDs, row.TargetID)
		default:
			return manifest, fmt.Errorf("unknown recovery target type %q", row.TargetType)
		}
	}
	manifest.TargetRootHash = recoveryTargetRootHash(manifest)
	return manifest, nil
}

func createRecoveryProofArtifact(db *gorm.DB, plan models.FeedRecoveryPlan, run models.FeedRecoveryRun, manifest recoveryPurgeManifest) (string, error) {
	// Exact target IDs live in feed_recovery_plan_targets. The provider proof is
	// intentionally constant-size for large resets and binds to that normalized
	// root/count instead of duplicating one unbounded JSON array.
	payload := map[string]interface{}{"version": 2, "run_id": run.PublicID, "plan_id": plan.PublicID, "manifest_hash": plan.ManifestHash, "target_root_hash": manifest.TargetRootHash, "target_scope": manifest.TargetScope, "target_count": plan.TargetCount, "tenant_id": plan.TenantID, "source_ids": manifest.SourceIDs, "no_full_rollback": plan.NoFullRollback, "created_at": time.Now().UTC()}
	raw, _ := json.Marshal(payload)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(raw); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	body := compressed.Bytes()
	sum := sha256.Sum256(body)
	checksum := hex.EncodeToString(sum[:])
	key := fmt.Sprintf("system/recovery/%s/%s/%s.json.gz", plan.TenantID, plan.PublicID.String(), checksum)
	request := map[string]string{"key": key, "sha256": checksum, "payload_base64": base64.StdEncoding.EncodeToString(body)}
	_, status, err := callAggregationInternal(http.MethodPost, "/internal/recovery-artifacts", request)
	if err != nil || status < 200 || status >= 300 {
		return "", fmt.Errorf("recovery proof artifact upload failed (%d): %v", status, err)
	}
	_, verifyStatus, verifyErr := callAggregationInternal(http.MethodPost, "/internal/recovery-artifacts/verify", map[string]string{"key": key, "sha256": checksum})
	if verifyErr != nil || verifyStatus < 200 || verifyStatus >= 300 {
		return "", fmt.Errorf("recovery proof artifact readback failed (%d): %v", verifyStatus, verifyErr)
	}
	artifact := models.FeedRecoveryArtifact{PlanID: plan.ID, TenantID: plan.TenantID, ArtifactType: "purge_manifest", ArtifactKey: key, SHA256: checksum, ByteSize: int64(len(body)), State: "verified", ExpiresAt: time.Now().UTC().Add(24 * time.Hour)}
	if err := db.Create(&artifact).Error; err != nil {
		return "", err
	}
	return key, nil
}

// ensureRecoveryProofArtifact makes the immutable manifest proof durable before
// the first destructive phase. A retry reuses the verified artifact recorded
// for the plan instead of uploading a second proof or changing its checksum.
func ensureRecoveryProofArtifact(db *gorm.DB, plan models.FeedRecoveryPlan, run models.FeedRecoveryRun, manifest recoveryPurgeManifest) (string, error) {
	if strings.TrimSpace(run.RecoveryArtifactRef) != "" {
		return run.RecoveryArtifactRef, nil
	}
	var artifact models.FeedRecoveryArtifact
	err := db.Where("plan_id=? AND artifact_type=?", plan.ID, "purge_manifest").First(&artifact).Error
	if err == nil {
		if artifact.State != "verified" || strings.TrimSpace(artifact.ArtifactKey) == "" {
			return "", fmt.Errorf("recovery proof artifact is not verified")
		}
		return artifact.ArtifactKey, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", err
	}
	return createRecoveryProofArtifact(db, plan, run, manifest)
}

func createFeedRecoveryTombstones(tx *gorm.DB, run models.FeedRecoveryRun, manifestHash string, items []models.ContentItem) error {
	rows := make([]models.NewsIngestTombstone, 0, len(items))
	for _, item := range items {
		identity, source, originalURL, err := retentionTombstoneIdentity(run.TenantID, item)
		if err != nil {
			return fmt.Errorf("recovery tombstone identity: %w", err)
		}
		runPublicID := run.PublicID
		rows = append(rows, models.NewsIngestTombstone{TenantID: run.TenantID, IdentityHash: identity, SourceIdentityHash: source, OriginalURLHash: originalURL, OriginalContentID: item.PublicID, ManifestHash: manifestHash, RecoveryRunID: &run.ID, RecoveryRunPublicID: &runPublicID, Reason: "feed_recovery_purge"})
	}
	if len(rows) == 0 {
		return nil
	}
	return tx.Create(&rows).Error
}

func purgeRecoveryNews(db *gorm.DB, run models.FeedRecoveryRun, plan models.FeedRecoveryPlan, manifest recoveryPurgeManifest) error {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return err
	}
	ids := uniqueUUIDs(manifest.NewsContentIDs)
	if len(ids) == 0 {
		return nil
	}
	var existing int64
	if err := db.Model(&models.FeedRecoveryAction{}).Where("run_id=? AND idempotency_key=?", run.ID, "purge-news:"+plan.ManifestHash).Count(&existing).Error; err != nil {
		return fmt.Errorf("check News purge idempotency: %w", err)
	}
	if existing > 0 {
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var items []models.ContentItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id IN ? AND type=? AND status=?", run.TenantID, ids, models.ContentTypeNews, models.ContentStatusReady).Find(&items).Error; err != nil || len(items) != len(ids) {
			return fmt.Errorf("News purge manifest is stale")
		}
		protected, protectionErr := retentionProtectedContentIDs(tx, run.TenantID, ids)
		if protectionErr != nil {
			return protectionErr
		}
		if len(protected) > 0 {
			return fmt.Errorf("News purge manifest contains protected content")
		}
		if issues, err := retentionDependencyPreflight(tx, run.TenantID, ids); err != nil {
			return err
		} else if err := retentionDependencyError(issues); err != nil {
			return err
		}
		if err := createFeedRecoveryTombstones(tx, run, plan.ManifestHash, items); err != nil {
			return err
		}
		if err := reconcileHistoricalTelemetry(tx, ids, time.Now().UTC()); err != nil {
			return err
		}
		if err := reconcileHistoricalRedundancy(tx, run.TenantID, ids, "feed-recovery", time.Now().UTC()); err != nil {
			return err
		}
		if result := tx.Where("tenant_id=? AND public_id IN ?", run.TenantID, ids).Delete(&models.ContentItem{}); result.Error != nil || result.RowsAffected != int64(len(ids)) {
			return fmt.Errorf("News purge did not match frozen manifest")
		}
		for _, storyID := range manifest.NewsStoryIDs {
			var remaining int64
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND story_id=?", run.TenantID, storyID).Count(&remaining).Error; err != nil {
				return err
			}
			if remaining == 0 {
				if err := reconcileHistoricalStories(tx, run.TenantID, []uuid.UUID{storyID}, time.Now().UTC()); err != nil {
					return err
				}
				if err := tx.Where("tenant_id=? AND public_id=?", run.TenantID, storyID).Delete(&models.Story{}).Error; err != nil {
					return err
				}
			}
		}
		if err := advanceNewsSnapshotGenerations(tx, run.TenantID); err != nil {
			return err
		}
		return tx.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "purge_news", State: "succeeded", IdempotencyKey: "purge-news:" + plan.ManifestHash, Evidence: datatypes.JSON([]byte(fmt.Sprintf(`{"count":%d,"sources_preserved":true}`, len(ids))))}).Error
	})
}

func requestRecoveryReseed(db *gorm.DB, run models.FeedRecoveryRun, plan models.FeedRecoveryPlan, manifest recoveryPurgeManifest, lane string) error {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return err
	}
	sourceIDs := manifest.SourceIDs
	// A tenant may intentionally have no active sources during an expected-empty
	// reset. Treat that lane as a durable no-op; availability and verification
	// still expose the empty/partial result to operators.
	if len(sourceIDs) == 0 {
		return nil
	}
	if lane == "news" || lane == "media" {
		payload := map[string]interface{}{"run_id": run.PublicID.String(), "tenant_id": run.TenantID, "lane": lane, "source_ids": sourceIDs, "lookback_hours": manifest.LookbackHours, "max_items": map[string]int{"news": manifest.NewsMaxItems, "media": manifest.MediaMaxItems}[lane], "manifest_hash": plan.ManifestHash, "idempotency_key": "reseed:" + plan.ManifestHash + ":" + lane, "preserve_checkpoints": true, "fencing_token": run.LaneLease}
		body, status, err := callAggregationInternal(http.MethodPost, "/internal/recovery/reseed", payload)
		if err != nil || status < 200 || status >= 300 {
			return fmt.Errorf("%s reseed request failed (%d): %v", lane, status, err)
		}
		var response struct {
			Data struct {
				FencingToken string `json:"fencing_token"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &response) != nil || response.Data.FencingToken != run.LaneLease {
			return fmt.Errorf("%s reseed fencing proof did not match the recovery lease", lane)
		}
	}
	return nil
}

func purgeRecoveryMedia(db *gorm.DB, run models.FeedRecoveryRun, plan models.FeedRecoveryPlan, manifest recoveryPurgeManifest) error {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return err
	}
	ids := uniqueUUIDs(manifest.MediaContentIDs)
	if len(ids) == 0 {
		return nil
	}
	// Freeze one durable saga item before any provider call. This is the
	// recovery boundary: a provider timeout can no longer erase the only record
	// of what was intended.
	err := db.Transaction(func(tx *gorm.DB) error {
		var items []models.ContentItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id IN ? AND type IN ? AND status=? AND is_feed_unit=TRUE AND feed_visibility=?", run.TenantID, ids, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, models.ContentStatusReady, feedVisibilityVisible).Find(&items).Error; err != nil || len(items) != len(ids) {
			if err != nil {
				return err
			}
			return fmt.Errorf("media purge manifest is stale")
		}
		protected, protectionErr := retentionProtectedContentIDs(tx, run.TenantID, ids)
		if protectionErr != nil {
			return protectionErr
		}
		if len(protected) > 0 {
			return fmt.Errorf("media purge manifest contains protected content")
		}
		for _, item := range items {
			objects := []string{}
			for _, value := range []*string{item.PlaybackURL, item.MediaURL, item.FallbackPlaybackURL, item.ThumbnailURL} {
				if value != nil && strings.TrimSpace(*value) != "" {
					objects = append(objects, strings.TrimSpace(*value))
				}
			}
			objectsJSON, _ := json.Marshal(objects)
			if len(objects) == 0 && !plan.NoFullRollback {
				return fmt.Errorf("media recovery map is unavailable for %s", item.PublicID)
			}
			itemHash := retentionSHA256(fmt.Sprintf("%s:%s:%s", plan.ManifestHash, item.PublicID, strings.Join(objects, "|")))
			var existing models.FeedRecoveryMediaPurgeItem
			findErr := tx.Where("run_id=? AND content_item_id=?", run.ID, item.PublicID).First(&existing).Error
			if findErr == nil {
				continue
			}
			if !errors.Is(findErr, gorm.ErrRecordNotFound) {
				return findErr
			}
			providerKey := "purge-media-item:" + plan.ManifestHash + ":" + item.PublicID.String()
			if err := tx.Create(&models.FeedRecoveryMediaPurgeItem{RunID: run.ID, PlanID: plan.ID, TenantID: run.TenantID, ContentItemID: item.PublicID, ManifestHash: plan.ManifestHash, ItemHash: itemHash, ProviderObjects: datatypes.JSON(objectsJSON), RecoveryMapPresent: len(objects) > 0, NoFullRollback: plan.NoFullRollback, ProviderIdempotencyKey: providerKey, State: "prepared"}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	var saga []models.FeedRecoveryMediaPurgeItem
	if err := db.Where("run_id=? AND state IN ?", run.ID, []string{"prepared", "object_delete_requested", "object_deleted", "cms_delete_pending", "cms_deleted", "blocked"}).Order("id ASC").Find(&saga).Error; err != nil {
		return err
	}
	if len(saga) == 0 {
		return nil
	}
	requestIDs := make([]uuid.UUID, 0, len(saga))
	for _, item := range saga {
		if item.State == "prepared" || item.State == "object_delete_requested" || item.State == "blocked" {
			requestIDs = append(requestIDs, item.ContentItemID)
		}
	}
	if len(requestIDs) > 0 {
		if err := db.Model(&models.FeedRecoveryMediaPurgeItem{}).Where("run_id=? AND state IN ?", run.ID, []string{"prepared", "blocked", "object_delete_requested"}).Updates(map[string]interface{}{"state": "object_delete_requested", "attempt_count": gorm.Expr("attempt_count + 1"), "last_error": nil, "updated_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
	}
	var response struct {
		Data struct {
			Results []struct {
				ID            string `json:"id"`
				Error         string `json:"error"`
				RequestID     string `json:"request_id"`
				FreedBytes    int64  `json:"freed_bytes"`
				DeletedCount  int    `json:"deleted_count"`
				ResultHash    string `json:"result_hash"`
				ObjectsAbsent bool   `json:"objects_absent"`
			} `json:"results"`
			ResultRoot   string `json:"result_root"`
			FencingToken string `json:"fencing_token"`
		} `json:"data"`
	}
	requestPrefixByID := map[uuid.UUID]string{}
	itemIdempotencyKeys := map[string]string{}
	sagaByID := make(map[uuid.UUID]models.FeedRecoveryMediaPurgeItem, len(saga))
	for _, item := range saga {
		sagaByID[item.ContentItemID] = item
		if strings.TrimSpace(item.ProviderIdempotencyKey) == "" {
			return fmt.Errorf("media saga item %s is missing its durable provider idempotency key", item.ContentItemID)
		}
		requestPrefixByID[item.ContentItemID] = item.ProviderIdempotencyKey
		itemIdempotencyKeys[item.ContentItemID.String()] = item.ProviderIdempotencyKey
	}
	for start := 0; start < len(requestIDs); start += 30 {
		end := start + 30
		if end > len(requestIDs) {
			end = len(requestIDs)
		}
		batch := requestIDs[start:end]
		idempotencyKey := "purge-media:" + plan.ManifestHash + ":batch:" + strconv.Itoa(start/30)
		sagaPayload := make([]map[string]interface{}, 0, len(batch))
		for _, id := range batch {
			item := sagaByID[id]
			var objects []string
			if err := json.Unmarshal(item.ProviderObjects, &objects); err != nil {
				return fmt.Errorf("media recovery map is unreadable for %s", id)
			}
			sagaPayload = append(sagaPayload, map[string]interface{}{"id": id, "provider_objects": objects, "no_full_rollback": item.NoFullRollback})
		}
		payload := map[string]interface{}{"run_id": run.PublicID.String(), "tenant_id": run.TenantID, "content_ids": batch, "saga_items": sagaPayload, "manifest_hash": plan.ManifestHash, "fencing_token": run.LaneLease, "idempotency_key": idempotencyKey, "item_idempotency_keys": itemIdempotencyKeys}
		body, status, callErr := callAggregationInternal(http.MethodPost, "/internal/recovery/purge-media", payload)
		var batchResponse struct {
			Data struct {
				Results []struct {
					ID            string `json:"id"`
					Error         string `json:"error"`
					RequestID     string `json:"request_id"`
					FreedBytes    int64  `json:"freed_bytes"`
					DeletedCount  int    `json:"deleted_count"`
					ResultHash    string `json:"result_hash"`
					ObjectsAbsent bool   `json:"objects_absent"`
				} `json:"results"`
				ResultRoot   string `json:"result_root"`
				FencingToken string `json:"fencing_token"`
			} `json:"data"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &batchResponse)
		}
		if callErr != nil && len(batchResponse.Data.Results) == 0 {
			return fmt.Errorf("media artifact purge failed (%d): %v", status, callErr)
		}
		if len(batchResponse.Data.Results) > 0 && batchResponse.Data.FencingToken != run.LaneLease {
			return fmt.Errorf("media provider result fencing token mismatch")
		}
		if len(batchResponse.Data.Results) > 0 {
			hashes := make([]string, 0, len(batchResponse.Data.Results))
			for _, result := range batchResponse.Data.Results {
				if result.ResultHash == "" {
					return fmt.Errorf("media provider result omitted checksum")
				}
				hashes = append(hashes, result.ResultHash)
			}
			sort.Strings(hashes)
			if batchResponse.Data.ResultRoot != retentionSHA256(strings.Join(hashes, "|")) {
				return fmt.Errorf("media provider result root mismatch")
			}
		}
		response.Data.Results = append(response.Data.Results, batchResponse.Data.Results...)
	}
	resultByID := map[uuid.UUID]struct {
		errorMessage, requestID, resultHash string
		deletedCount                        int
		freedBytes                          int64
		objectsAbsent                       bool
	}{}
	for _, result := range response.Data.Results {
		id, parseErr := uuid.Parse(result.ID)
		if parseErr == nil {
			resultByID[id] = struct {
				errorMessage, requestID, resultHash string
				deletedCount                        int
				freedBytes                          int64
				objectsAbsent                       bool
			}{result.Error, result.RequestID, result.ResultHash, result.DeletedCount, result.FreedBytes, result.ObjectsAbsent}
		}
	}
	var firstErr error
	for _, item := range saga {
		if item.State == "cms_deleted" {
			now := time.Now().UTC()
			if err := db.Model(&item).Updates(map[string]interface{}{"state": "verified", "verified_at": now, "updated_at": now, "last_error": nil}).Error; err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if err := db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "purge_media_item", State: "succeeded", IdempotencyKey: "purge-media-item:" + plan.ManifestHash + ":" + item.ContentItemID.String(), Evidence: datatypes.JSON([]byte(fmt.Sprintf(`{"content_item_id":"%s","provider_deleted":true,"cms_deleted":true,"reconciled":true}`, item.ContentItemID)))}).Error; err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		providerAlreadyDeleted := item.State == "object_deleted" || item.State == "cms_delete_pending"
		providerRequestID := ""
		if !providerAlreadyDeleted {
			providerResult, providerOK := resultByID[item.ContentItemID]
			providerRequestID = providerResult.requestID
			itemErr := providerResult.errorMessage
			expectedRequestID := requestPrefixByID[item.ContentItemID]
			expectedResultHash := retentionSHA256(fmt.Sprintf("%s|%s|%d|%d|%t", plan.ManifestHash, expectedRequestID, providerResult.deletedCount, providerResult.freedBytes, providerResult.objectsAbsent))
			if providerOK && (providerResult.requestID != expectedRequestID || providerResult.resultHash != expectedResultHash) {
				itemErr = "provider result proof mismatch"
			}
			if providerOK && !providerResult.objectsAbsent {
				itemErr = "provider object readback is not empty"
			}
			if !providerOK {
				itemErr = "provider response omitted saga item"
			}
			if itemErr != "" || !providerOK {
				message := itemErr
				if message == "" {
					message = "provider deletion failed"
				}
				if persistErr := db.Model(&item).Updates(map[string]interface{}{"state": "blocked", "last_error": message, "updated_at": time.Now().UTC()}).Error; persistErr != nil && firstErr == nil {
					firstErr = persistErr
				}
				if firstErr == nil {
					firstErr = fmt.Errorf("media purge item %s: %s", item.ContentItemID, message)
				}
				continue
			}
			objectDeletedAt := time.Now().UTC()
			if err := db.Model(&item).Updates(map[string]interface{}{"state": "object_deleted", "provider_request_id": providerResult.requestID, "provider_result_hash": providerResult.resultHash, "provider_idempotency_key": requestPrefixByID[item.ContentItemID], "object_deleted_at": objectDeletedAt, "updated_at": objectDeletedAt}).Error; err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		// Provider success is durable. For object_deleted/CMS-pending retries,
		// resume only this CMS half and never call the provider again.
		if err := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&models.FeedRecoveryMediaPurgeItem{}).Where("id=?", item.ID).Update("state", "cms_delete_pending").Error; err != nil {
				return err
			}
			result := tx.Where("tenant_id=? AND public_id=?", run.TenantID, item.ContentItemID).Delete(&models.ContentItem{})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected > 1 {
				return fmt.Errorf("media purge deleted more than one CMS row")
			}
			now := time.Now().UTC()
			return tx.Model(&models.FeedRecoveryMediaPurgeItem{}).Where("id=?", item.ID).Updates(map[string]interface{}{"state": "cms_deleted", "cms_deleted_at": now, "verified_at": now, "updated_at": now}).Error
		}); err != nil {
			_ = db.Model(&item).Updates(map[string]interface{}{"state": "cms_delete_pending", "last_error": err.Error(), "updated_at": time.Now().UTC()}).Error
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if providerRequestID == "" {
			var persisted models.FeedRecoveryMediaPurgeItem
			if db.Where("id=?", item.ID).First(&persisted).Error == nil {
				providerRequestID = persisted.ProviderRequestID
			}
		}
		if err := db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "purge_media_item", State: "succeeded", IdempotencyKey: "purge-media-item:" + plan.ManifestHash + ":" + item.ContentItemID.String(), Evidence: datatypes.JSON([]byte(fmt.Sprintf(`{"content_item_id":"%s","provider_deleted":true,"cms_deleted":true,"provider_request_id":%q}`, item.ContentItemID, providerRequestID)))}).Error; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func executePurgeReseedRun(db *gorm.DB, run models.FeedRecoveryRun, plan models.FeedRecoveryPlan) (string, error) {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return "partial", err
	}
	if run.Phase == "verification_wait" || run.Phase == "verifying_probe_2" {
		return "verifying_probe_2", nil
	}
	manifest, err := decodeRecoveryPurgeManifest(plan)
	if err != nil {
		return "", err
	}
	if err := verifyRecoveryPlanTargets(db, plan, manifest); err != nil {
		return "partial", err
	}
	manifest, err = hydrateRecoveryManifestTargets(db, plan, manifest)
	if err != nil {
		return "partial", err
	}
	artifactRef, err := ensureRecoveryProofArtifact(db, plan, run, manifest)
	if err != nil {
		return "partial", err
	}
	if strings.TrimSpace(run.RecoveryArtifactRef) == "" {
		if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"recovery_artifact_ref": artifactRef, "destructive_manifest": plan.PurgeManifest, "updated_at": time.Now().UTC()}); err != nil {
			return "partial", err
		}
		run.RecoveryArtifactRef = artifactRef
	}
	if run.ExpectedEmpty {
		if err := setRecoveryAvailabilityForRun(db, run, "expected_empty"); err != nil {
			return "partial", err
		}
	} else {
		if err := setRecoveryAvailabilityForRun(db, run, "refreshing"); err != nil {
			return "partial", err
		}
	}
	lanes := recoveryLanes(run.Lane)
	current := run.DestructiveLane
	if current == "" {
		current = lanes[0]
	}
	start := 0
	for i, lane := range lanes {
		if lane == current {
			start = i
			break
		}
	}
	for _, lane := range lanes[start:] {
		if lane == "news" {
			if err := purgeRecoveryNews(db, run, plan, manifest); err != nil {
				return "partial", err
			}
		} else {
			if err := purgeRecoveryMedia(db, run, plan, manifest); err != nil {
				return "partial", err
			}
		}
		reseedKey := "reseed:" + plan.ManifestHash + ":" + lane
		var reseedExists int64
		if err := db.Model(&models.FeedRecoveryAction{}).Where("run_id=? AND idempotency_key=?", run.ID, reseedKey).Count(&reseedExists).Error; err != nil {
			return "partial", fmt.Errorf("check %s reseed idempotency: %w", lane, err)
		}
		if reseedExists == 0 {
			if err := requestRecoveryReseed(db, run, plan, manifest, lane); err != nil {
				return "partial", err
			}
			if err := db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "reseed_" + lane, State: "succeeded", IdempotencyKey: reseedKey, Evidence: datatypes.JSON([]byte(fmt.Sprintf(`{"checkpoint_mode":"preserve","lookback_hours":%d}`, manifest.LookbackHours)))}).Error; err != nil {
				return "partial", fmt.Errorf("record %s reseed ledger: %w", lane, err)
			}
		}
		if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"destructive_lane": lane, "phase": "reseeding_" + lane, "heartbeat_at": time.Now().UTC(), "updated_at": time.Now().UTC()}); err != nil {
			return "partial", err
		}
	}
	if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "verifying_probe_1", "destructive_lane": "", "heartbeat_at": time.Now().UTC(), "updated_at": time.Now().UTC()}); err != nil {
		return "partial", err
	}
	return "verifying_probe_1", nil
}

func ExecuteFeedRecoveryRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req struct {
		ReauthProof string `json:"reauth_proof"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "execution confirmation required"})
		return
	}
	// Validate the fresh password proof before claiming a run or acquiring any
	// recovery lease. Plan approval is phrase/JWT-only; this proof is required
	// only at the final Execute action.
	var pendingRun models.FeedRecoveryRun
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&pendingRun).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	var plan models.FeedRecoveryPlan
	if err := db.Where("tenant_id=? AND id=?", principal.TenantID, pendingRun.PlanID).First(&plan).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "recovery plan not found"})
		return
	}
	secret, err := utils.GetJWTSecret()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "execution re-auth unavailable"})
		return
	}
	proof, err := utils.ParseFeedRecoveryReauthProof(strings.TrimSpace(req.ReauthProof), secret)
	if err != nil || proof.UserID != principal.UserID || proof.TenantID != principal.TenantID || proof.PlanID != plan.PublicID.String() || proof.ManifestHash != plan.ManifestHash {
		c.JSON(http.StatusForbidden, gin.H{"error": "fresh execution confirmation does not match this plan"})
		return
	}
	run, err := claimRecoveryRun(db, principal.TenantID, id)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if plan.Level == "purge_reseed" && !principal.HasRole("admin") {
		if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "blocked", "outcome": "admin_role_required", "claim_token": nil, "claim_expires_at": nil, "lane_lease": ""}); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "recovery block could not be fenced"})
			return
		}
		if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
			return
		}
		c.JSON(http.StatusForbidden, gin.H{"error": "admin role required for Purge & Reseed"})
		return
	}
	if plan.Level == "purge_reseed" {
		if capErr := requireRetentionCapability(db, principal.TenantID, retentionCapabilityRecoveryPurge); capErr != nil {
			if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "blocked", "outcome": "safety_remediation_required", "error": capErr.Error(), "claim_token": nil, "claim_expires_at": nil, "lane_lease": ""}); err != nil {
				c.JSON(http.StatusConflict, gin.H{"error": "recovery safety block could not be fenced"})
				return
			}
			if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
				return
			}
			c.JSON(http.StatusConflict, gin.H{"error": capErr.Error()})
			return
		}
	}
	if plan.Level == "rotate" && run.Lane == "both" {
		if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "blocked", "outcome": "both_requires_sequential_lane_execution", "claim_token": nil, "claim_expires_at": nil, "updated_at": time.Now().UTC()}); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "recovery block could not be fenced"})
			return
		}
		if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": "Both-lane rotate is blocked until sequential lane execution is enabled"})
		return
	}
	if plan.Level == "rotate" {
		if capErr := requireRetentionCapability(db, principal.TenantID, retentionCapabilityRecoveryRotate); capErr != nil {
			if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "blocked", "outcome": "safety_remediation_required", "error": capErr.Error(), "claim_token": nil, "claim_expires_at": nil, "lane_lease": ""}); err != nil {
				c.JSON(http.StatusConflict, gin.H{"error": "recovery safety block could not be fenced"})
				return
			}
			if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
				return
			}
			c.JSON(http.StatusConflict, gin.H{"error": capErr.Error()})
			return
		}
	}
	if plan.Level == "purge_reseed" {
		phase, purgeErr := executePurgeReseedRun(db, run, plan)
		if purgeErr != nil {
			if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "partial", "outcome": "purge_reseed_failed", "error": purgeErr.Error(), "claim_token": nil, "claim_expires_at": nil, "lane_lease": ""}); err != nil {
				c.JSON(http.StatusConflict, gin.H{"error": "purge failure could not be fenced"})
				return
			}
			if err := setRecoveryAvailabilityForRun(db, run, "partial"); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be recorded"})
				return
			}
			if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
				return
			}
			c.JSON(http.StatusConflict, gin.H{"error": purgeErr.Error()})
			return
		}
		run.Phase = phase
	} else {
		if err := setRecoveryAvailabilityForRun(db, run, "refreshing"); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "recovery availability could not be established"})
			return
		}
	}
	if plan.Level == "repair" {
		if repairErr := runRegisteredRecoveryRepairTools(db, run); repairErr != nil {
			if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "partial", "outcome": "repair_failed", "error": repairErr.Error(), "claim_token": nil, "claim_expires_at": nil, "lane_lease": ""}); err != nil {
				c.JSON(http.StatusConflict, gin.H{"error": "repair failure could not be fenced"})
				return
			}
			if err := setRecoveryAvailabilityForRun(db, run, "partial"); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be recorded"})
				return
			}
			if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
				return
			}
			c.JSON(http.StatusConflict, gin.H{"error": "registered recovery repair failed"})
			return
		}
	}
	if plan.Level == "rotate" {
		if run.Phase == "executing" && run.CandidateGenerationID == nil {
			if err := buildRecoveryCandidate(db, run); err != nil {
				if fenceErr := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "failed", "outcome": "failed", "error": err.Error()}); fenceErr != nil {
					c.JSON(http.StatusConflict, gin.H{"error": "candidate failure could not be fenced"})
					return
				}
				if availabilityErr := setRecoveryAvailabilityForRun(db, run, "partial"); availabilityErr != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be recorded"})
					return
				}
				if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
					return
				}
				c.JSON(http.StatusConflict, gin.H{"error": "candidate build failed"})
				return
			}
			// buildRecoveryCandidate persists the generation pointers in the
			// transaction. Reload before catch-up so retries and concurrent
			// workers use the durable state, not the pre-claim copy.
			if err := db.First(&run, run.ID).Error; err != nil {
				if fenceErr := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "failed", "outcome": "failed", "error": err.Error()}); fenceErr != nil {
					c.JSON(http.StatusConflict, gin.H{"error": "candidate reload failure could not be fenced"})
					return
				}
				if availabilityErr := setRecoveryAvailabilityForRun(db, run, "partial"); availabilityErr != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be recorded"})
					return
				}
				if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
					return
				}
				c.JSON(http.StatusConflict, gin.H{"error": "candidate state could not be reloaded"})
				return
			}
		}
		if err := markRecoveryCandidateCaughtUp(db, run); err != nil {
			if fenceErr := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "failed", "outcome": "failed", "error": err.Error(), "claim_token": nil, "claim_expires_at": nil}); fenceErr != nil {
				c.JSON(http.StatusConflict, gin.H{"error": "candidate catch-up failure could not be fenced"})
				return
			}
			if availabilityErr := setRecoveryAvailabilityForRun(db, run, "partial"); availabilityErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be recorded"})
				return
			}
			if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
				return
			}
			c.JSON(http.StatusConflict, gin.H{"error": "candidate catch-up failed"})
			return
		}
	}
	pass := 1
	if run.Phase == "verification_wait" {
		pass = 2
	}
	if run.Phase != "verification_wait" {
		if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "verifying_probe_1", "heartbeat_at": time.Now().UTC()}); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "verification phase could not be fenced"})
			return
		}
	} else {
		if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "verifying_probe_2", "heartbeat_at": time.Now().UTC()}); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "verification phase could not be fenced"})
			return
		}
	}
	clean := runRecoveryVerification(db, run, pass)
	if !clean {
		if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "partial", "outcome": "verification_failed", "error": "verification gate did not pass", "claim_token": nil, "claim_expires_at": nil, "lane_lease": "", "updated_at": time.Now().UTC()}); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "verification failure could not be fenced"})
			return
		}
		if err := setRecoveryAvailabilityForRun(db, run, "partial"); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be recorded"})
			return
		}
		if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": "verification gate did not pass; no cutover occurred"})
		return
	}
	if pass == 1 {
		due := time.Now().UTC().Add(5 * time.Minute)
		if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "verification_wait", "verification_due_at": due, "claim_token": nil, "claim_expires_at": nil, "lane_lease": "", "heartbeat_at": time.Now().UTC(), "updated_at": time.Now().UTC()}); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "verification wait could not be persisted"})
			return
		}
		if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"data": gin.H{"run": run.PublicID, "phase": "verification_wait", "verification_due_at": due}})
		return
	}
	if plan.Level == "rotate" {
		if err := cutoverRecoveryCandidate(db, run); err != nil {
			if fenceErr := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": "partial", "outcome": "cutover_failed", "error": err.Error()}); fenceErr != nil {
				c.JSON(http.StatusConflict, gin.H{"error": "cutover failure could not be fenced"})
				return
			}
			if availabilityErr := setRecoveryAvailabilityForRun(db, run, "partial"); availabilityErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be recorded"})
				return
			}
			if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
				return
			}
			c.JSON(http.StatusConflict, gin.H{"error": "safe cutover failed"})
			return
		}
	}
	terminalPhase := "succeeded"
	if plan.Level == "rotate" {
		terminalPhase = "rollback_ready"
	}
	if err := updateRecoveryRunWithLease(db, run, map[string]interface{}{"phase": terminalPhase, "outcome": "succeeded", "error": nil, "claim_token": nil, "claim_expires_at": nil, "lane_lease": "", "verification_due_at": nil, "updated_at": time.Now().UTC()}); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "terminal recovery state could not be fenced"})
		return
	}
	if err := setRecoveryAvailabilityForRun(db, run, "normal"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be cleared"})
		return
	}
	if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
		return
	}
	data := gin.H{"run": run.PublicID, "phase": terminalPhase}
	if plan.Level == "rotate" {
		data["rollback_deadline"] = time.Now().UTC().Add(recoveryRollbackTTL)
	}
	c.JSON(http.StatusOK, gin.H{"data": data})
}

func CancelFeedRecoveryRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedRecoveryRun
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	if run.Phase != "cancel_window" {
		c.JSON(http.StatusConflict, gin.H{"error": "run is no longer cancellable"})
		return
	}
	now := time.Now().UTC()
	if err := db.Transaction(func(tx *gorm.DB) error {
		if result := tx.Model(&run).Where("phase = ?", "cancel_window").Updates(map[string]interface{}{"phase": "cancelled", "outcome": "cancelled", "claim_token": nil, "claim_expires_at": nil, "lane_lease": "", "updated_at": now}); result.Error != nil || result.RowsAffected != 1 {
			if result.Error != nil {
				return result.Error
			}
			return fmt.Errorf("run is no longer cancellable")
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if err := setRecoveryAvailabilityForRun(db, run, "normal"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be cleared"})
		return
	}
	if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
		return
	}
	if err := db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "cancel", State: "succeeded", IdempotencyKey: "cancel:" + run.PublicID.String(), Evidence: datatypes.JSON([]byte(`{"cancelled":true}`))}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cancel ledger could not be recorded"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": run.PublicID})
}

func RollbackFeedRecoveryRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedRecoveryRun
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	if run.Outcome != "succeeded" || run.ActiveGenerationID == nil || run.CandidateGenerationID == nil || run.RollbackDeadline == nil || !run.RollbackDeadline.After(time.Now().UTC()) {
		c.JSON(http.StatusConflict, gin.H{"error": "rollback window is closed or no cutover exists"})
		return
	}
	now := time.Now().UTC()
	err = db.Transaction(func(tx *gorm.DB) error {
		var locked models.FeedRecoveryRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", run.ID, run.TenantID).First(&locked).Error; err != nil {
			return err
		}
		laneToken, err := acquireRecoveryLaneLeases(tx, locked, now)
		if err != nil {
			return err
		}
		if err := tx.Model(&locked).Where("id=?", locked.ID).Update("lane_lease", laneToken).Error; err != nil {
			return err
		}
		run.LaneLease = laneToken
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", run.TenantID, run.Lane).First(&head).Error; err != nil {
			return err
		}
		if head.ActiveGenerationID == nil || *head.ActiveGenerationID != *run.CandidateGenerationID {
			return fmt.Errorf("active generation no longer matches run")
		}
		if err := tx.Model(&models.FeedGeneration{}).Where("public_id=?", *run.CandidateGenerationID).Updates(map[string]interface{}{"state": "retired", "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.FeedGeneration{}).Where("public_id=?", *run.ActiveGenerationID).Updates(map[string]interface{}{"state": "active", "rollback_deadline": nil, "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&head).Updates(map[string]interface{}{"active_generation_id": *run.ActiveGenerationID, "generation": gorm.Expr("generation + 1"), "updated_at": now}).Error; err != nil {
			return err
		}
		result := tx.Model(&models.FeedRecoveryRun{}).Where("id=? AND tenant_id=? AND lane_lease=?", run.ID, run.TenantID, run.LaneLease).Updates(map[string]interface{}{"phase": "rolled_back", "outcome": "rolled_back", "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("rollback lost its fencing lease")
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if err := setRecoveryAvailabilityForRun(db, run, "normal"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "recovery availability could not be cleared"})
		return
	}
	if !releaseRecoveryLaneLeasesOrRespond(c, db, run) {
		return
	}
	if err := db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "rollback", State: "succeeded", IdempotencyKey: "rollback:" + run.PublicID.String(), Evidence: datatypes.JSON([]byte(`{"within_24h":true}`))}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "rollback ledger could not be recorded"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run.PublicID, "phase": "rolled_back"}})
}

func cutoverRecoveryCandidate(db *gorm.DB, run models.FeedRecoveryRun) error {
	if err := requireRecoveryLaneLease(db, run); err != nil {
		return err
	}
	if run.ActiveGenerationID == nil || run.CandidateGenerationID == nil {
		return fmt.Errorf("generation pointers are incomplete")
	}
	now := time.Now().UTC()
	deadline := now.Add(recoveryRollbackTTL)
	return db.Transaction(func(tx *gorm.DB) error {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", run.TenantID, run.Lane).First(&head).Error; err != nil {
			return err
		}
		if head.CandidateGenerationID == nil || *head.CandidateGenerationID != *run.CandidateGenerationID {
			return fmt.Errorf("candidate no longer matches this run")
		}
		if err := tx.Model(&models.FeedGeneration{}).Where("public_id=?", *run.ActiveGenerationID).Updates(map[string]interface{}{"state": "rollback", "rollback_deadline": deadline, "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.FeedGeneration{}).Where("public_id=?", *run.CandidateGenerationID).Updates(map[string]interface{}{"state": "active", "cutover_at": now, "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&head).Updates(map[string]interface{}{"active_generation_id": *run.CandidateGenerationID, "candidate_generation_id": nil, "generation": gorm.Expr("generation + 1"), "updated_at": now}).Error; err != nil {
			return err
		}
		result := tx.Model(&models.FeedRecoveryRun{}).Where("id=? AND tenant_id=? AND lane_lease=?", run.ID, run.TenantID, run.LaneLease).Updates(map[string]interface{}{"rollback_deadline": deadline, "phase": "rollback_ready", "outcome": "succeeded", "verification_due_at": nil, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("cutover lost its fencing lease")
		}
		return nil
	})
}

// feedAvailabilityMeta is intentionally safe for public clients: it contains
// neither quota nor deletion details.
type feedAvailabilityMeta struct {
	Availability      string `json:"availability"`
	MessageKey        string `json:"message_key,omitempty"`
	RetryAfterSeconds *int   `json:"retry_after_seconds,omitempty"`
}

func currentFeedAvailability(db *gorm.DB, tenant, lane string) *feedAvailabilityMeta {
	var state models.FeedAvailabilityState
	if err := db.Where("tenant_id=? AND lane=?", tenant, lane).First(&state).Error; err != nil || state.State == "normal" {
		return nil
	}
	return &feedAvailabilityMeta{Availability: state.State, MessageKey: state.MessageKey, RetryAfterSeconds: state.RetryAfterSeconds}
}

// applyActiveGenerationMembership makes a generation head a real serving
// boundary. During the migration window (before the recovery tables exist) it
// is a no-op so older installations keep their existing live query behavior.
func applyActiveGenerationMembership(db *gorm.DB, query *gorm.DB, tenant, lane, memberType, memberColumn string) *gorm.DB {
	return feedcontract.ApplyActiveGenerationMembership(db, query, tenant, lane, memberType, memberColumn)
}

// attachReadyNewsStoryToGeneration retains the News-side write path. Media
// units use feedstate.SyncMediaMembership in the same transaction as their
// lifecycle mutation; News membership remains story-classifier owned.
func attachReadyNewsStoryToGeneration(db *gorm.DB, item models.ContentItem) {
	if item.Type != models.ContentTypeNews || item.Status != models.ContentStatusReady || item.StoryID == nil {
		return
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", item.TenantID, "news").First(&head).Error; err != nil {
		return
	}
	for _, generationID := range []*uuid.UUID{head.ActiveGenerationID, head.CandidateGenerationID} {
		if generationID == nil || *generationID == uuid.Nil {
			continue
		}
		_ = db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING", *generationID, "story", *item.StoryID).Error
	}
}
