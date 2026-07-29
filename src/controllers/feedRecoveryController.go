package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const feedRecoveryPlanTTL = 30 * time.Minute

type feedRecoveryPlanRequest struct {
	Lane           string `json:"lane"`
	Level          string `json:"level"`
	CapacityMode   string `json:"capacity_mode"`
	NoFullRollback bool   `json:"no_full_rollback"`
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
	checksum, count, err := recoverySourceProof(db, tenant, lane)
	if err != nil {
		return models.FeedRecoveryPlan{}, err
	}
	evidence := map[string]interface{}{"source_checksum": checksum, "source_count": count, "execution_installed": false, "safe_note": "Slice 7 only establishes recovery identity; no content or source mutation is enabled."}
	manifest := map[string]interface{}{"tenant": tenant, "lane": lane, "level": level, "capacity_mode": mode, "source_checksum": checksum, "source_count": count}
	manifestHash := recoveryHash(manifest)
	planHash := recoveryHash(map[string]interface{}{"manifest": manifest, "no_full_rollback": req.NoFullRollback})
	evidenceJSON, _ := json.Marshal(evidence)
	policyJSON, _ := json.Marshal(map[string]string{"execution": "disabled_until_slice_8"})
	return models.FeedRecoveryPlan{TenantID: tenant, Lane: lane, Level: level, CapacityMode: mode, State: "awaiting_approval", PlanHash: planHash, ManifestHash: manifestHash, SourceChecksum: checksum, SourceCount: count, Evidence: datatypes.JSON(evidenceJSON), PolicySnapshot: datatypes.JSON(policyJSON), NoFullRollback: req.NoFullRollback, ExpiresAt: time.Now().UTC().Add(feedRecoveryPlanTTL), CreatedBy: actor}, nil
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
	if err := db.Create(&plan).Error; err != nil {
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
	if err := db.Create(&plan).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to refresh preflight"})
		return
	}
	c.JSON(http.StatusCreated, plan)
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
		Phrase      string `json:"phrase"`
		ReauthProof string `json:"reauth_proof"`
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
	secret, err := utils.GetJWTSecret()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "re-auth unavailable"})
		return
	}
	proof, err := utils.ParseFeedRecoveryReauthProof(strings.TrimSpace(req.ReauthProof), secret)
	if err != nil || proof.UserID != principal.UserID || proof.TenantID != principal.TenantID || proof.PlanID != plan.PublicID.String() || proof.ManifestHash != plan.ManifestHash {
		c.JSON(http.StatusForbidden, gin.H{"error": "fresh re-auth proof does not match this plan"})
		return
	}
	expectedPhrase := "APPROVE FEED RECOVERY " + strings.ToUpper(plan.ManifestHash[:12])
	if strings.TrimSpace(req.Phrase) != expectedPhrase {
		c.JSON(http.StatusBadRequest, gin.H{"error": "confirmation phrase does not match plan"})
		return
	}
	now := time.Now().UTC()
	notBefore := now.Add(time.Minute)
	phraseHash := recoveryHash(req.Phrase)
	approval := models.FeedRecoveryApproval{PlanID: plan.ID, TenantID: plan.TenantID, Actor: principal.Email, PlanHash: plan.PlanHash, ManifestHash: plan.ManifestHash, TargetCount: plan.TargetCount, PhraseProofHash: phraseHash, ReauthJTI: proof.ID, NoFullRollback: plan.NoFullRollback, ApprovedAt: now, ConsumedAt: &now}
	run := models.FeedRecoveryRun{PlanID: plan.ID, TenantID: plan.TenantID, Lane: plan.Lane, CorrelationID: uuid.New(), Phase: "cancel_window", NotBefore: &notBefore}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&approval).Error; err != nil {
			return err
		}
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		if err := tx.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "approval", State: "succeeded", IdempotencyKey: "approval:" + plan.PlanHash, Evidence: datatypes.JSON([]byte(`{"fresh_reauth":true,"cancel_window_seconds":60}`))}).Error; err != nil {
			return err
		}
		return tx.Model(&plan).Update("state", "approved").Error
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "approval proof was already consumed or approval could not be saved"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"plan": plan.PublicID, "run": run, "cancel_deadline": notBefore})
}

func ListFeedRecoveryRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var runs []models.FeedRecoveryRun
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id=?", principal.TenantID).Order("created_at DESC").Limit(100).Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list runs"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": runs})
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
	var run models.FeedRecoveryRun
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
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

// attachReadyContentToFeedGenerations is the single dual-write primitive for
// future Safe Cutover. Normal serving remains live; this only records which
// active/candidate namespaces have seen a newly eligible item.
func attachReadyContentToFeedGenerations(db *gorm.DB, item models.ContentItem) {
	if item.Status != models.ContentStatusReady {
		return
	}
	lane, memberType, memberID := "media", "feed_unit", item.PublicID
	if item.Type == models.ContentTypeNews {
		if item.StoryID == nil {
			return
		}
		lane, memberType, memberID = "news", "story", *item.StoryID
	} else if !item.IsFeedUnit || item.FeedVisibility != "visible" {
		return
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", item.TenantID, lane).First(&head).Error; err != nil {
		return
	}
	for _, generationID := range []*uuid.UUID{head.ActiveGenerationID, head.CandidateGenerationID} {
		if generationID == nil {
			continue
		}
		_ = db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING", *generationID, memberType, memberID).Error
	}
}
