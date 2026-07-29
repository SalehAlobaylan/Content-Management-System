package controllers

// Historical retirement is deliberately separate from current-month
// compaction. It can touch only an exact approved manifest from completed
// months whose Month in Review is finalized; source records are never targets.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func historicalIDs(raw datatypes.JSON) ([]uuid.UUID, error) {
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	result := make([]uuid.UUID, 0, len(values))
	for _, value := range values {
		id, err := uuid.Parse(value)
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, nil
}
func historicalManifestHash(tenant, timezone string, content, stories []uuid.UUID) string {
	sort.Slice(content, func(i, j int) bool { return content[i].String() < content[j].String() })
	sort.Slice(stories, func(i, j int) bool { return stories[i].String() < stories[j].String() })
	raw, _ := json.Marshal(map[string]interface{}{"v": 1, "tenant": tenant, "timezone": timezone, "content": content, "stories": stories})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type historicalCursor struct {
	OrderedAt time.Time `json:"ordered_at"`
	ContentID uuid.UUID `json:"content_id"`
}

func encodeHistoricalCursor(item models.ContentItem) string {
	orderedAt := item.CreatedAt
	if item.PublishedAt != nil {
		orderedAt = *item.PublishedAt
	}
	raw, _ := json.Marshal(historicalCursor{OrderedAt: orderedAt.UTC(), ContentID: item.PublicID})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeHistoricalCursor(raw string) (historicalCursor, error) {
	if strings.TrimSpace(raw) == "" {
		return historicalCursor{}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return historicalCursor{}, fmt.Errorf("invalid historical cursor: %w", err)
	}
	var cursor historicalCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil || cursor.ContentID == uuid.Nil || cursor.OrderedAt.IsZero() {
		return historicalCursor{}, errors.New("invalid historical cursor")
	}
	return cursor, nil
}

func finalizedArchiveMonths(db *gorm.DB, tenant string) (map[string]bool, error) {
	var archives []models.NewsMonthArchive
	if err := db.Where("tenant_id=? AND state=?", tenant, "finalized").Find(&archives).Error; err != nil {
		return nil, err
	}
	months := map[string]bool{}
	for _, archive := range archives {
		months[archive.MonthStart.UTC().Format("2006-01-02")] = true
	}
	return months, nil
}

// historicalProtectedContentIDs narrows only the telemetry exception promised
// by the historical policy: old delivery telemetry may expire after roll-up,
// while likes, bookmarks, comments, holds, flags and open moderation never do.
func historicalProtectedContentIDs(db *gorm.DB, tenant string, candidates []uuid.UUID, now time.Time) (map[uuid.UUID]bool, error) {
	protected := make(map[uuid.UUID]bool)
	if len(candidates) == 0 {
		return protected, nil
	}
	var interactionIDs []uuid.UUID
	if err := db.Model(&models.UserInteraction{}).
		Where("content_item_id IN ? AND (type IN ? OR created_at >= ?)", candidates, []models.InteractionType{models.InteractionTypeLike, models.InteractionTypeBookmark, models.InteractionTypeComment}, now.AddDate(0, 0, -90)).
		Pluck("content_item_id", &interactionIDs).Error; err != nil {
		return nil, err
	}
	for _, id := range interactionIDs {
		protected[id] = true
	}
	var ids []uuid.UUID
	if err := db.Model(&models.RetentionHold{}).Where("tenant_id=? AND target_type=? AND target_id IN ? AND released_at IS NULL AND (expires_at IS NULL OR expires_at > ?)", tenant, "content", candidates, now).Pluck("target_id", &ids).Error; err != nil {
		return nil, err
	}
	for _, id := range ids {
		protected[id] = true
	}
	ids = nil
	if err := db.Raw(`
		SELECT DISTINCT c.public_id
		FROM content_items c
		JOIN retention_holds h ON h.target_id = c.story_id
		WHERE c.tenant_id = ? AND c.public_id IN ?
		  AND h.tenant_id = ? AND h.target_type = 'story' AND h.released_at IS NULL
		  AND (h.expires_at IS NULL OR h.expires_at > ?)`, tenant, candidates, tenant, now).Scan(&ids).Error; err != nil {
		return nil, err
	}
	for _, id := range ids {
		protected[id] = true
	}
	ids = nil
	if err := db.Model(&models.ModerationReport{}).Where("tenant_id=? AND target_type=? AND target_id IN ? AND status=?", tenant, models.ModerationTargetContent, candidates, "open").Pluck("target_id", &ids).Error; err != nil {
		return nil, err
	}
	for _, id := range ids {
		protected[id] = true
	}
	ids = nil
	if err := db.Model(&models.ContentFlag{}).Where("tenant_id=? AND content_item_id IN ?", tenant, candidates).Pluck("content_item_id", &ids).Error; err != nil {
		return nil, err
	}
	for _, id := range ids {
		protected[id] = true
	}
	return protected, nil
}

func historicalDependencyPreflight(db *gorm.DB, tenant string, ids []uuid.UUID) (map[string]int64, error) {
	issues, err := retentionDependencyPreflight(db, tenant, ids)
	if err != nil {
		return nil, err
	}
	// Historical retirement owns and journals these two relationships below.
	delete(issues, "active_redundancy_members")
	delete(issues, "redundancy_pairs")
	return issues, nil
}

func historicalCandidates(db *gorm.DB, tenant, timezone, cursor string, maxRows int, maxBytes int64) ([]models.ContentItem, []uuid.UUID, int64, string, bool, error) {
	months, err := finalizedArchiveMonths(db, tenant)
	if err != nil {
		return nil, nil, 0, "", false, err
	}
	if len(months) == 0 {
		return nil, nil, 0, "", false, nil
	}
	location := monthlyReviewLocation(timezone)
	monthPredicates := make([]string, 0, len(months))
	monthArgs := make([]interface{}, 0, len(months)*2)
	for monthKey := range months {
		monthStart, parseErr := time.Parse("2006-01-02", monthKey)
		if parseErr != nil {
			return nil, nil, 0, "", false, fmt.Errorf("invalid finalized archive month %q: %w", monthKey, parseErr)
		}
		start := time.Date(monthStart.Year(), monthStart.Month(), 1, 0, 0, 0, 0, location).UTC()
		end := start.In(location).AddDate(0, 1, 0).UTC()
		monthPredicates = append(monthPredicates, "COALESCE(published_at, created_at) >= ? AND COALESCE(published_at, created_at) < ?")
		monthArgs = append(monthArgs, start, end)
	}
	parsedCursor, err := decodeHistoricalCursor(cursor)
	if err != nil {
		return nil, nil, 0, "", false, err
	}
	probeLimit := 1000
	if maxRows > 0 && maxRows < probeLimit {
		probeLimit = maxRows * 2
		if probeLimit < 100 {
			probeLimit = 100
		}
	}
	query := db.Select("id, public_id, tenant_id, type, status, story_id, published_at, created_at").
		Where("tenant_id=? AND type=? AND status IN ?", tenant, models.ContentTypeNews, []models.ContentStatus{models.ContentStatusReady, models.ContentStatusFailed, models.ContentStatusArchived}).
		Where("("+strings.Join(monthPredicates, " OR ")+")", monthArgs...).
		Where("NOT EXISTS (SELECT 1 FROM news_month_archive_stories nas JOIN news_month_archives na ON na.id = nas.archive_id WHERE na.tenant_id = ? AND na.state = 'finalized' AND nas.lead_content_id = content_items.public_id)", tenant).
		Order("COALESCE(published_at, created_at) ASC, public_id ASC").
		Limit(probeLimit + 1)
	if !parsedCursor.OrderedAt.IsZero() {
		query = query.Where("(COALESCE(published_at, created_at) > ? OR (COALESCE(published_at, created_at) = ? AND public_id > ?))", parsedCursor.OrderedAt, parsedCursor.OrderedAt, parsedCursor.ContentID)
	}
	var items []models.ContentItem
	if err := query.Find(&items).Error; err != nil {
		return nil, nil, 0, "", false, err
	}
	fetchedHasMore := len(items) > probeLimit
	if fetchedHasMore {
		items = items[:probeLimit]
	}
	ids := extractPublicIDs(items)
	protected, err := historicalProtectedContentIDs(db, tenant, ids, time.Now().UTC())
	if err != nil {
		return nil, nil, 0, "", fetchedHasMore, err
	}
	// Estimate only the bounded metadata page. We walk the original ordered
	// page below so protected/oversized rows advance the cursor, while the first
	// row that would exceed this batch remains at the continuation boundary.
	if len(ids) > 0 {
		var bytes []struct {
			ID    uuid.UUID `gorm:"column:public_id"`
			Bytes int64     `gorm:"column:bytes"`
		}
		if err := db.Model(&models.ContentItem{}).Select("public_id, pg_column_size(content_items)::bigint AS bytes").Where("tenant_id=? AND public_id IN ?", tenant, ids).Find(&bytes).Error; err != nil {
			return nil, nil, 0, "", fetchedHasMore, err
		}
		byID := make(map[uuid.UUID]int64, len(bytes))
		for _, row := range bytes {
			byID[row.ID] = row.Bytes
		}
		bounded := make([]models.ContentItem, 0, maxRows)
		var running int64
		pageCursor := ""
		stoppedForCap := false
		for _, item := range items {
			if protected[item.PublicID] {
				pageCursor = encodeHistoricalCursor(item)
				continue
			}
			itemBytes := byID[item.PublicID]
			if itemBytes > maxBytes {
				// A single oversized row cannot be allowed to block every later
				// candidate. It is excluded with an auditable bounded scan and the
				// continuation advances beyond it.
				pageCursor = encodeHistoricalCursor(item)
				continue
			}
			if len(bounded) >= maxRows || (len(bounded) > 0 && running+itemBytes > maxBytes) {
				stoppedForCap = true
				break
			}
			bounded = append(bounded, item)
			running += itemBytes
			pageCursor = encodeHistoricalCursor(item)
		}
		items = bounded
		hasMore := fetchedHasMore || stoppedForCap
		ids = extractPublicIDs(items)
		storySet := map[uuid.UUID]bool{}
		for _, item := range items {
			if item.StoryID != nil {
				storySet[*item.StoryID] = true
			}
		}
		var estimated int64
		if len(ids) > 0 {
			if err := db.Raw("SELECT COALESCE(SUM(pg_column_size(content_items)),0)::bigint FROM content_items WHERE tenant_id=? AND public_id IN ?", tenant, ids).Scan(&estimated).Error; err != nil {
				return nil, nil, 0, pageCursor, hasMore, err
			}
		}
		stories := make([]uuid.UUID, 0, len(storySet))
		for id := range storySet {
			stories = append(stories, id)
		}
		return items, stories, estimated, pageCursor, hasMore, nil
	}
	return nil, nil, 0, "", fetchedHasMore, nil
}

func historicalCandidateCursor(items []models.ContentItem) map[string]string {
	if len(items) == 0 {
		return map[string]string{}
	}
	last := items[len(items)-1]
	stamp := last.CreatedAt
	if last.PublishedAt != nil {
		stamp = *last.PublishedAt
	}
	return map[string]string{"ordered_at": stamp.UTC().Format(time.RFC3339Nano), "public_id": last.PublicID.String()}
}

func PrepareHistoricalRetention(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var request struct {
		Cursor string `json:"cursor"`
	}
	if c.Request != nil && c.Request.Body != nil && c.Request.ContentLength != 0 {
		if err := c.ShouldBindJSON(&request); err != nil && !errors.Is(err, io.EOF) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid historical cursor request"})
			return
		}
	}
	policy := loadRetentionPolicy(db, principal.TenantID)
	timezone := retentionNewsTimezone(db, principal.TenantID)
	run, err := runRetention(db, principal.TenantID, "manual", principal.Email)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	items, stories, estimated, nextCursor, hasMore, err := historicalCandidates(db, principal.TenantID, timezone, request.Cursor, policy.MaxRowsPerRun, policy.MaxBytesPerRun)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	ids := extractPublicIDs(items)
	if len(ids) == 0 {
		response := gin.H{"error": "no archive-gated, unprotected historical News is eligible"}
		if hasMore && nextCursor != "" {
			response["next_cursor"] = nextCursor
		}
		c.JSON(409, response)
		return
	}
	// historicalCandidates already returns a deterministic bounded prefix. A
	// second assertion protects against future callers widening that helper.
	if len(ids) > policy.MaxRowsPerRun || estimated > policy.MaxBytesPerRun {
		c.JSON(409, gin.H{"error": "historical batch exceeds policy cap"})
		return
	}
	issues, err := historicalDependencyPreflight(db, principal.TenantID, ids)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if err := retentionDependencyError(issues); err != nil {
		c.JSON(409, gin.H{"error": err.Error()})
		return
	}
	hash := historicalManifestHash(principal.TenantID, timezone, ids, stories)
	now := time.Now().UTC()
	manifest := models.RetentionHistoricalManifest{RunID: run.ID, TenantID: principal.TenantID, PolicyVersion: policy.PolicyVersion, Timezone: timezone, ManifestHash: hash, State: "prepared", ContentIDs: idsJSON(ids), StoryIDs: idsJSON(stories), ContentCount: len(ids), StoryCount: len(stories), EstimatedBytes: estimated, ExpiresAt: now.Add(24 * time.Hour), Evidence: retentionActionEvidence(map[string]interface{}{"archive_gate": "finalized", "source_targets": 0, "dependency_issues": issues, "protected_rows_excluded": len(items) - len(ids), "next_cursor": nextCursor, "has_more": hasMore, "cursor_order": "coalesce(published_at,created_at),public_id"})}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&manifest).Error; err != nil {
			return err
		}
		action := models.RetentionAction{RunID: run.ID, TenantID: principal.TenantID, ActionClass: models.RetentionActionPrepareHistoricalPurge, OwnerSystem: "news_database", TargetScope: "historical-manifest:" + manifest.PublicID.String(), Mode: models.RetentionModeAssist, Decision: "approval_required", Outcome: models.RetentionActionApprovalRequired, IdempotencyKey: "retention-historical:" + hash, EvidenceFingerprint: hash, ManifestHash: &hash, TargetCount: len(ids), EstimatedBytes: estimated, Guardrail: "finalized_archive_exact_manifest_no_sources", Evidence: manifest.Evidence}
		if err := tx.Create(&action).Error; err != nil {
			return err
		}
		manifest.ActionID = &action.ID
		return tx.Model(&manifest).Update("action_id", action.ID).Error
	})
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	retentionAudit(db, principal, "retention.historical.prepare", manifest.PublicID.String(), "success", nil)
	c.JSON(201, gin.H{"data": manifest})
}

func createHistoricalTombstones(tx *gorm.DB, tenant string, action models.RetentionAction, manifest models.RetentionHistoricalManifest, items []models.ContentItem) error {
	rows := make([]models.NewsIngestTombstone, 0, len(items))
	actionID := action.ID
	for _, item := range items {
		identity, source, _, err := retentionTombstoneIdentity(tenant, item)
		if err != nil {
			return fmt.Errorf("historical tombstone preflight: %w", err)
		}
		rows = append(rows, models.NewsIngestTombstone{TenantID: tenant, IdentityHash: identity, SourceIdentityHash: source, OriginalContentID: item.PublicID, ManifestHash: manifest.ManifestHash, RetirementActionID: &actionID, Reason: "historical_retention"})
	}
	return tx.Create(&rows).Error
}

func prepareHistoricalRecoveryArtifact(tenant string, action models.RetentionAction, manifest models.RetentionHistoricalManifest, items []models.ContentItem, now time.Time) (models.RetentionHistoricalRecoveryArtifact, error) {
	// Reuse the bounded payload/upload/readback protocol, but persist the
	// pointer in the historical ledger rather than the compaction ledger.
	shim := models.RetentionCompactionManifest{ID: 0, PublicID: manifest.PublicID, ManifestHash: manifest.ManifestHash, CreatedAt: manifest.CreatedAt}
	artifact, err := prepareRecoveryArtifact(tenant, action, shim, items, extractPublicIDs(items), now)
	if err != nil {
		return models.RetentionHistoricalRecoveryArtifact{}, err
	}
	return models.RetentionHistoricalRecoveryArtifact{ActionID: action.ID, ManifestID: manifest.ID, TenantID: tenant, ArtifactKey: artifact.ArtifactKey, SHA256: artifact.SHA256, CompressedBytes: artifact.CompressedBytes, UncompressedBytes: artifact.UncompressedBytes, State: artifact.State, ExpiresAt: artifact.ExpiresAt, VerifiedAt: artifact.VerifiedAt}, nil
}

func reconcileHistoricalTelemetry(tx *gorm.DB, ids []uuid.UUID, now time.Time) error {
	var interactions []models.UserInteraction
	if err := tx.Where("content_item_id IN ? AND type NOT IN ? AND created_at < ?", ids, []models.InteractionType{models.InteractionTypeLike, models.InteractionTypeBookmark, models.InteractionTypeComment}, now.AddDate(0, 0, -90)).Find(&interactions).Error; err != nil {
		return err
	}
	interactionIDs := make([]uuid.UUID, 0, len(interactions))
	for _, interaction := range interactions {
		interactionIDs = append(interactionIDs, interaction.PublicID)
	}
	if len(interactionIDs) == 0 {
		return nil
	}
	if err := tx.Where("interaction_public_id IN ?", interactionIDs).Delete(&models.ConsumerRequestIdempotency{}).Error; err != nil {
		return err
	}
	return tx.Where("public_id IN ?", interactionIDs).Delete(&models.UserInteraction{}).Error
}

func reconcileHistoricalRedundancy(tx *gorm.DB, tenant string, ids []uuid.UUID, actor string, now time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	retiring := map[uuid.UUID]bool{}
	for _, id := range ids {
		retiring[id] = true
	}
	var families []models.RedundancyFamily
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Joins("JOIN redundancy_family_members m ON m.family_id = redundancy_families.id AND m.ended_at IS NULL").Where("redundancy_families.tenant_id=? AND redundancy_families.status='active' AND m.content_item_id IN ?", tenant, ids).Find(&families).Error; err != nil {
		return err
	}
	uniqueFamilies := make([]models.RedundancyFamily, 0, len(families))
	seenFamilies := map[uint]bool{}
	for _, family := range families {
		if !seenFamilies[family.ID] {
			seenFamilies[family.ID] = true
			uniqueFamilies = append(uniqueFamilies, family)
		}
	}
	for _, family := range uniqueFamilies {
		var members []models.RedundancyFamilyMember
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("family_id=? AND ended_at IS NULL", family.ID).Find(&members).Error; err != nil {
			return err
		}
		var survivor *models.RedundancyFamilyMember
		for index := range members {
			if !retiring[members[index].ContentItemID] && (survivor == nil || members[index].ContentItemID.String() < survivor.ContentItemID.String()) {
				survivor = &members[index]
			}
		}
		metadata := retentionActionEvidence(map[string]interface{}{"historical_retirement": true, "family_id": family.PublicID.String(), "retiring_content_ids": ids})
		if retiring[family.CanonicalContentItemID] && survivor != nil {
			if err := tx.Model(&models.RedundancyFamily{}).Where("id=?", family.ID).Update("canonical_content_item_id", survivor.ContentItemID).Error; err != nil {
				return err
			}
			if err := tx.Model(&models.RedundancyFamilyMember{}).Where("id=?", survivor.ID).Update("role", "canonical").Error; err != nil {
				return err
			}
			metadata = retentionActionEvidence(map[string]interface{}{"historical_retirement": true, "family_id": family.PublicID.String(), "operation": "rehome", "new_canonical_content_id": survivor.ContentItemID.String()})
		} else if survivor == nil {
			if err := tx.Model(&models.RedundancyFamily{}).Where("id=?", family.ID).Updates(map[string]interface{}{"status": "dissolved", "dissolved_at": now, "dissolved_by": actor, "dissolve_reason": "historical_retention"}).Error; err != nil {
				return err
			}
			metadata = retentionActionEvidence(map[string]interface{}{"historical_retirement": true, "family_id": family.PublicID.String(), "operation": "dissolve"})
		}
		if err := tx.Create(&models.RedundancyAction{TenantID: tenant, FamilyID: &family.ID, ActionKind: "historical_retirement_reconcile", Actor: actor, Outcome: "completed", Reason: "approved historical retention", Metadata: metadata, IdempotencyKey: "historical-retention-family:" + family.PublicID.String()}).Error; err != nil {
			return err
		}
	}
	var pairs []models.RedundancyPair
	if err := tx.Where("tenant_id=? AND (item_a_id IN ? OR item_b_id IN ?)", tenant, ids, ids).Find(&pairs).Error; err != nil {
		return err
	}
	for _, pair := range pairs {
		metadata := retentionActionEvidence(map[string]interface{}{"historical_retirement": true, "pair_id": pair.PublicID.String(), "latest_evaluation_id": pair.LatestEvaluationID})
		if err := tx.Create(&models.RedundancyAction{TenantID: tenant, PairID: &pair.ID, FamilyID: pair.FamilyID, ActionKind: "historical_retirement_pair", Actor: actor, Outcome: "retired", Reason: "approved historical retention", Metadata: metadata, IdempotencyKey: "historical-retention-pair:" + pair.PublicID.String()}).Error; err != nil {
			return err
		}
	}
	if err := tx.Where("tenant_id=? AND (item_a_id IN ? OR item_b_id IN ?)", tenant, ids, ids).Delete(&models.RedundancyPair{}).Error; err != nil {
		return err
	}
	return tx.Where("tenant_id=? AND content_item_id IN ?", tenant, ids).Delete(&models.RedundancyFamilyMember{}).Error
}

func reconcileHistoricalStories(tx *gorm.DB, tenant string, storyIDs []uuid.UUID, now time.Time) error {
	for _, storyID := range storyIDs {
		var rss []models.RSSFeed
		if err := tx.Where("tenant_id=? AND story_id=?", tenant, storyID).Find(&rss).Error; err != nil {
			return err
		}
		for _, feed := range rss {
			if err := tx.Model(&models.RSSFeed{}).Where("id=?", feed.ID).Updates(map[string]interface{}{"enabled": false, "story_id": nil}).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&models.NewsStoryOverride{}).Where("tenant_id=? AND story_id=?", tenant, storyID).Update("expires_at", now).Error; err != nil {
			return err
		}
		var all []models.Story
		if err := tx.Where("tenant_id=? AND public_id NOT IN ?", tenant, storyIDs).Find(&all).Error; err != nil {
			return err
		}
		for _, story := range all {
			var related []string
			if json.Unmarshal(story.RelatedIDs, &related) != nil {
				continue
			}
			changed := false
			out := related[:0]
			for _, id := range related {
				if id == storyID.String() {
					changed = true
					continue
				}
				out = append(out, id)
			}
			if changed {
				raw, _ := json.Marshal(out)
				if err := tx.Model(&models.Story{}).Where("id=?", story.ID).Update("related_ids", datatypes.JSON(raw)).Error; err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func ExecuteHistoricalRetention(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(400, gin.H{"error": "invalid action id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadRetentionPolicy(db, principal.TenantID)
	if !policy.Enabled || policy.Mode != models.RetentionModeAssist {
		c.JSON(409, gin.H{"error": "historical purge requires enabled Assist mode"})
		return
	}
	if err := requireRetentionCapability(db, principal.TenantID, retentionCapabilityHistorical); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	now := time.Now().UTC()
	var action models.RetentionAction
	var manifest models.RetentionHistoricalManifest
	var externalArtifactKey string
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&action).Error; err != nil {
			return err
		}
		if action.ActionClass != models.RetentionActionPrepareHistoricalPurge || action.Outcome != models.RetentionActionApproved {
			return errors.New("approved historical manifest action is required")
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("action_id=? AND state=?", action.ID, "approved").First(&manifest).Error; err != nil {
			return err
		}
		if now.After(manifest.ExpiresAt) {
			return errors.New("historical manifest expired")
		}
		contentIDs, err := historicalIDs(manifest.ContentIDs)
		if err != nil {
			return err
		}
		storyIDs, err := historicalIDs(manifest.StoryIDs)
		if err != nil {
			return err
		}
		var items []models.ContentItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id IN ?", principal.TenantID, contentIDs).Find(&items).Error; err != nil || len(items) != len(contentIDs) {
			return errors.New("historical manifest is stale")
		}
		for _, item := range items {
			if item.Type != models.ContentTypeNews || (item.Status != models.ContentStatusReady && item.Status != models.ContentStatusFailed && item.Status != models.ContentStatusArchived) {
				return errors.New("historical manifest acquired an ineligible content state")
			}
		}
		protected, err := historicalProtectedContentIDs(tx, principal.TenantID, contentIDs, now)
		if err != nil {
			return err
		}
		if len(protected) > 0 {
			return errors.New("historical candidate gained a protected reference")
		}
		if issues, e := historicalDependencyPreflight(tx, principal.TenantID, contentIDs); e != nil {
			return e
		} else if e = retentionDependencyError(issues); e != nil {
			return e
		}
		artifact, err := prepareHistoricalRecoveryArtifact(principal.TenantID, action, manifest, items, now)
		if err != nil {
			return err
		}
		externalArtifactKey = artifact.ArtifactKey
		if err := tx.Create(&artifact).Error; err != nil {
			return err
		}
		if err := createHistoricalTombstones(tx, principal.TenantID, action, manifest, items); err != nil {
			return err
		}
		if err := reconcileHistoricalTelemetry(tx, contentIDs, now); err != nil {
			return err
		}
		if err := reconcileHistoricalRedundancy(tx, principal.TenantID, contentIDs, principal.Email, now); err != nil {
			return err
		}
		if result := tx.Where("tenant_id=? AND public_id IN ?", principal.TenantID, contentIDs).Delete(&models.ContentItem{}); result.Error != nil || result.RowsAffected != int64(len(contentIDs)) {
			return errors.New("historical delete did not match manifest")
		}
		for _, storyID := range storyIDs {
			var remaining int64
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND story_id=?", principal.TenantID, storyID).Count(&remaining).Error; err != nil {
				return err
			}
			if remaining == 0 {
				if err := reconcileHistoricalStories(tx, principal.TenantID, []uuid.UUID{storyID}, now); err != nil {
					return err
				}
				if err := tx.Where("tenant_id=? AND public_id=?", principal.TenantID, storyID).Delete(&models.Story{}).Error; err != nil {
					return err
				}
			}
		}
		if err := advanceNewsSnapshotGenerations(tx, principal.TenantID); err != nil {
			return err
		}
		if err := tx.Model(&models.RetentionHistoricalManifest{}).Where("id=?", manifest.ID).Update("state", "executed").Error; err != nil {
			return err
		}
		return tx.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"action_class": models.RetentionActionExecuteHistoricalPurge, "outcome": models.RetentionActionToolSucceeded, "started_at": now, "finished_at": now, "recovery_ref": artifact.ArtifactKey, "guardrail": "approved_archive_gated_historical_manifest_recovery_verified"}).Error
	})
	if err != nil {
		if externalArtifactKey != "" {
			_ = deleteRecoveryArtifact(externalArtifactKey)
		}
		c.JSON(409, gin.H{"error": err.Error()})
		return
	}
	evictLocalNewsSnapshots(principal.TenantID)
	verification := map[string]interface{}{"phase": "historical_readback", "manifest_hash": manifest.ManifestHash, "recovery_artifact": externalArtifactKey}
	for _, window := range []string{models.NewsWindowToday, models.NewsWindowWeek, models.NewsWindowMonth} {
		if _, err := buildNewsSnapshot(db, principal.TenantID, window); err != nil {
			verification["snapshot_error"] = err.Error()
			_ = db.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerifyFailed, "verification": retentionActionEvidence(verification), "finished_at": time.Now().UTC()}).Error
			c.JSON(http.StatusConflict, gin.H{"error": "historical purge needs snapshot readback: " + err.Error()})
			return
		}
	}
	var surviving int64
	if err := db.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id IN ?", principal.TenantID, mustHistoricalIDs(manifest.ContentIDs)).Count(&surviving).Error; err != nil || surviving != 0 {
		if err != nil {
			verification["readback_error"] = err.Error()
		} else {
			verification["surviving_manifest_rows"] = surviving
		}
		_ = db.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerifyFailed, "verification": retentionActionEvidence(verification), "finished_at": time.Now().UTC()}).Error
		c.JSON(http.StatusConflict, gin.H{"error": "historical purge readback did not match manifest"})
		return
	}
	integrity, integrityErr := runFeedIntegrity(db, principal.TenantID, feedIntegrityRunOptions{Tier: models.FeedIntegrityTierDeep, Trigger: "retention_historical", CreatedBy: principal.Email})
	if integrityErr != nil {
		verification["feed_integrity_error"] = integrityErr.Error()
	} else {
		verification["feed_integrity_run_id"] = integrity.PublicID.String()
		verification["feed_integrity_status"] = integrity.Status
	}
	health, _ := collectSystemHealthSnapshot(db)
	verification["system_health"] = health.Overall
	if err := db.Exec("VACUUM (ANALYZE) content_items").Error; err != nil {
		verification["maintenance_error"] = err.Error()
		_ = db.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerifyFailed, "verification": retentionActionEvidence(verification), "finished_at": time.Now().UTC()}).Error
		c.JSON(http.StatusConflict, gin.H{"error": "historical purge needs maintenance verification: " + err.Error()})
		return
	}
	sample, sampleErr := collectRetentionDBSample(db, principal.TenantID)
	if sampleErr != nil {
		verification["measurement_error"] = sampleErr.Error()
	} else {
		verification["after_database_bytes"] = sample.DatabaseBytes
	}
	if integrityErr != nil || integrity.Status != models.FeedIntegrityRunCompleted || integrity.Headline != "all_clear" || health.Overall != "healthy" || sampleErr != nil {
		_ = db.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerifyFailed, "verification": retentionActionEvidence(verification), "finished_at": time.Now().UTC()}).Error
		c.JSON(http.StatusConflict, gin.H{"error": "historical purge completed but post-deploy safety verification failed"})
		return
	}
	_ = db.Model(&models.RetentionAction{}).Where("id=?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerified, "after_bytes": sample.DatabaseBytes, "verification": retentionActionEvidence(verification), "finished_at": time.Now().UTC()}).Error
	_ = db.Where("id=?", action.ID).First(&action).Error
	retentionAudit(db, principal, "retention.historical.execute", id.String(), "success", nil)
	c.JSON(http.StatusOK, gin.H{"data": action})
}

func mustHistoricalIDs(raw datatypes.JSON) []uuid.UUID {
	ids, err := historicalIDs(raw)
	if err != nil {
		return nil
	}
	return ids
}

func CreateRetentionMaintenanceReport(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadRetentionPolicy(db, principal.TenantID)
	sample, err := collectRetentionDBSample(db, principal.TenantID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	var request struct {
		ProviderBytes            *int64     `json:"provider_bytes"`
		ProviderSource           string     `json:"provider_source"`
		ProviderMeasuredAt       *time.Time `json:"provider_measured_at"`
		PhysicalReclaimConfirmed bool       `json:"physical_reclaim_confirmed"`
	}
	if c.Request.Body != nil {
		if err := c.ShouldBindJSON(&request); err != nil && !errors.Is(err, io.EOF) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid provider measurement payload"})
			return
		}
	}
	if request.ProviderBytes != nil && *request.ProviderBytes < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "provider_bytes must be non-negative"})
		return
	}
	if request.ProviderBytes != nil {
		if request.ProviderSource == "" {
			request.ProviderSource = "operator_readback"
		}
		if request.ProviderSource != "operator_readback" && request.ProviderSource != "neon_api" && request.ProviderSource != "supabase_api" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "provider_source is not supported"})
			return
		}
		if request.ProviderMeasuredAt == nil {
			now := time.Now().UTC()
			request.ProviderMeasuredAt = &now
		}
		sample.ProviderBytes = request.ProviderBytes
		sample.ProviderSource = request.ProviderSource
		sample.ProviderMeasuredAt = request.ProviderMeasuredAt
		if err := db.Model(&models.RetentionDBSample{}).Where("id = ?", sample.ID).Updates(map[string]interface{}{"provider_bytes": request.ProviderBytes, "provider_source": request.ProviderSource, "provider_measured_at": request.ProviderMeasuredAt}).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "could not persist provider measurement"})
			return
		}
	}
	var sparseColumn bool
	if err := db.Raw("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='content_items' AND column_name='embedding_sparse')").Scan(&sparseColumn).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	var sparse int64
	if sparseColumn {
		if err := db.Raw("SELECT COUNT(*) FROM content_items WHERE embedding_sparse IS NOT NULL").Scan(&sparse).Error; err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
	}
	now := time.Now().UTC()
	providerFresh := sample.ProviderBytes != nil && sample.ProviderMeasuredAt != nil &&
		sample.ProviderSource != "unavailable" && !sample.ProviderMeasuredAt.UTC().After(now.Add(5*time.Minute)) && now.Sub(sample.ProviderMeasuredAt.UTC()) <= 24*time.Hour
	postgresReady := sample.DatabaseBytes <= policy.DatabaseTargetBytes && sparse == 0
	providerReady := providerFresh && *sample.ProviderBytes <= policy.DatabaseTargetBytes
	var activeRetentionRuns, activeRecoveryLanes int64
	if err := db.Model(&models.RetentionRun{}).Where("tenant_id=? AND status=?", principal.TenantID, models.RetentionRunRunning).Count(&activeRetentionRuns).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not verify active retention runs"})
		return
	}
	if err := db.Table("feed_availability_states").Where("tenant_id=? AND state <> 'normal'", principal.TenantID).Count(&activeRecoveryLanes).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not verify active recovery lanes"})
		return
	}
	state := "not_ready"
	if postgresReady && providerReady && request.PhysicalReclaimConfirmed && activeRetentionRuns == 0 && activeRecoveryLanes == 0 {
		state = "free_downgrade_ready"
	}
	blocking := []string{}
	if !postgresReady {
		blocking = append(blocking, "postgres_or_sparse_not_ready")
	}
	if !providerFresh {
		blocking = append(blocking, "provider_measurement_missing_or_stale")
	}
	if !providerReady {
		blocking = append(blocking, "provider_bytes_above_target")
	}
	if !request.PhysicalReclaimConfirmed {
		blocking = append(blocking, "physical_reclaim_readback_missing")
	}
	if activeRetentionRuns > 0 {
		blocking = append(blocking, "active_retention_run")
	}
	if activeRecoveryLanes > 0 {
		blocking = append(blocking, "active_recovery_lane")
	}
	report := models.RetentionMaintenanceReport{
		TenantID: principal.TenantID, DatabaseBytes: sample.DatabaseBytes, TargetBytes: policy.DatabaseTargetBytes,
		SparseUseCount: sparse, ProviderBytes: sample.ProviderBytes, ProviderSource: sample.ProviderSource,
		ProviderMeasuredAt: sample.ProviderMeasuredAt, ProviderFresh: providerFresh,
		PostgresReady: postgresReady, ProviderReady: providerReady, BlockingReasons: datatypes.JSON(rawJSON(blocking)), State: state,
		Evidence: retentionActionEvidence(map[string]interface{}{
			"vacuum": "operator_only", "hnsw_toast": "operator_maintenance_required",
			"physical_reclaim_confirmed": request.PhysicalReclaimConfirmed,
			"active_retention_runs":      activeRetentionRuns,
			"active_recovery_lanes":      activeRecoveryLanes,
			"blocking_reasons":           blocking, "provider_source": sample.ProviderSource,
		}),
	}
	if err := db.Create(&report).Error; err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(201, gin.H{"data": report})
}
