package controllers

// This file prepares the exact evidence needed for a later bounded compaction
// executor. It deliberately does not mutate content_items, stories,
// interactions, or snapshots.

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

const retentionActionPrepareNewsCompaction = "news_database.prepare_compaction"

type retentionManifestStory struct {
	StoryID           uuid.UUID   `json:"story_id"`
	LeadID            uuid.UUID   `json:"lead_id"`
	RepresentativeIDs []uuid.UUID `json:"representative_ids"`
	ProtectedIDs      []uuid.UUID `json:"protected_ids"`
	RetireIDs         []uuid.UUID `json:"retire_ids"`
}

type retentionManifestPayload struct {
	TenantID       string                   `json:"tenant_id"`
	PolicyVersion  int                      `json:"policy_version"`
	Timezone       string                   `json:"timezone"`
	Stories        []retentionManifestStory `json:"stories"`
	BlockedStories map[string]string        `json:"blocked_stories,omitempty"`
	SelectedCount  int                      `json:"selected_story_count"`
	NextCursor     string                   `json:"next_cursor,omitempty"`
	HasMore        bool                     `json:"has_more"`
}

type retentionStoryCursor struct {
	LastMemberAt time.Time `json:"last_member_at"`
	StoryID      uuid.UUID `json:"story_id"`
}

type retentionStoryCandidate struct {
	StoryID      uuid.UUID `gorm:"column:story_id"`
	LastMemberAt time.Time `gorm:"column:last_member_at"`
}

func encodeRetentionStoryCursor(candidate retentionStoryCandidate) string {
	raw, _ := json.Marshal(retentionStoryCursor{LastMemberAt: candidate.LastMemberAt.UTC(), StoryID: candidate.StoryID})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeRetentionStoryCursor(raw string) (retentionStoryCursor, error) {
	if strings.TrimSpace(raw) == "" {
		return retentionStoryCursor{}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return retentionStoryCursor{}, fmt.Errorf("invalid retention cursor: %w", err)
	}
	var cursor retentionStoryCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil || cursor.StoryID == uuid.Nil || cursor.LastMemberAt.IsZero() {
		return retentionStoryCursor{}, errors.New("invalid retention cursor")
	}
	return cursor, nil
}

// Legacy News rows predate the Retention columns. NULL/NULL is their
// compatibility representation of full/full_member; all new News writes stamp
// the explicit values at the CMS boundary.
func retentionItemIsFull(item models.ContentItem) bool {
	state := derefStr(item.NewsRetentionState)
	role := derefStr(item.NewsFeedRole)
	return (state == "" && role == "") || (state == "full" && role == "full_member")
}

// retentionDependencyPreflight classifies relationships whose deletion semantics
// are not owned by the bounded News executor. Any result is a hard block: it is
// safer to leave a row in full fidelity than to silently cascade a transcript,
// media artifact ledger, or redundancy relationship.
func retentionDependencyPreflight(db *gorm.DB, tenant string, contentIDs []uuid.UUID) (map[string]int64, error) {
	issues := map[string]int64{}
	if len(contentIDs) == 0 {
		return issues, nil
	}
	checks := []struct {
		name  string
		query *gorm.DB
	}{
		{"inline_media_or_transcript", db.Model(&models.ContentItem{}).Where("tenant_id = ? AND public_id IN ? AND (transcript_id IS NOT NULL OR parent_content_item_id IS NOT NULL OR NULLIF(BTRIM(COALESCE(media_url, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(playback_url, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fallback_playback_url, '')), '') IS NOT NULL)", tenant, contentIDs)},
		{"transcript_rows", db.Table("transcripts").Where("content_item_id IN ?", contentIDs)},
		{"media_atomization_runs", db.Table("media_atomization_runs").Where("parent_content_item_id IN ?", contentIDs)},
		{"media_storage_artifacts", db.Table("media_storage_artifact_events").Where("content_item_id IN ? OR parent_content_item_id IN ?", contentIDs, contentIDs)},
		{"active_redundancy_members", db.Table("redundancy_family_members").Where("tenant_id = ? AND content_item_id IN ? AND ended_at IS NULL", tenant, contentIDs)},
		{"redundancy_pairs", db.Table("redundancy_pairs").Where("tenant_id = ? AND (item_a_id IN ? OR item_b_id IN ?)", tenant, contentIDs, contentIDs)},
	}
	for _, check := range checks {
		var count int64
		if err := check.query.Count(&count).Error; err != nil {
			return nil, fmt.Errorf("%s dependency check: %w", check.name, err)
		}
		if count > 0 {
			issues[check.name] = count
		}
	}
	return issues, nil
}

func retentionDependencyError(issues map[string]int64) error {
	if len(issues) == 0 {
		return nil
	}
	keys := make([]string, 0, len(issues))
	for key := range issues {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, issues[key]))
	}
	return fmt.Errorf("dependency preflight blocked compaction (%s)", strings.Join(parts, ", "))
}

func retentionEligibleStoryIDs(db *gorm.DB, tenant, timezone string, limit int) ([]uuid.UUID, error) {
	candidates, _, err := retentionEligibleStoryPage(db, tenant, timezone, "", limit)
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.StoryID)
	}
	return ids, nil
}

func retentionEligibleStoryPage(db *gorm.DB, tenant, timezone, cursor string, limit int) ([]retentionStoryCandidate, bool, error) {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		location = time.FixedZone("Asia/Riyadh", 3*60*60)
	}
	now := time.Now().In(location)
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, location).UTC()
	dormantCutoff := now.AddDate(0, 0, -7).UTC()
	weekStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, location).
		AddDate(0, 0, -int(now.Weekday())).UTC()
	if limit < 1 {
		limit = 1
	}
	parsedCursor, err := decodeRetentionStoryCursor(cursor)
	if err != nil {
		return nil, false, err
	}
	querySQL := `
		SELECT s.public_id AS story_id, s.last_member_at
		FROM stories s
		WHERE s.tenant_id = ?
		  AND COALESCE(s.news_retention_state, 'full') = 'full'
		  AND s.last_member_at >= ?
		  AND s.last_member_at <= ?
		  AND s.last_member_at < ?
		  AND NOT EXISTS (
			SELECT 1 FROM content_items pending
			WHERE pending.tenant_id = s.tenant_id AND pending.story_id = s.public_id
			  AND pending.status IN ('PENDING', 'PROCESSING')
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM retention_holds hold
			WHERE hold.tenant_id = s.tenant_id AND hold.target_type = 'story'
			  AND hold.target_id = s.public_id AND hold.released_at IS NULL
			  AND (hold.expires_at IS NULL OR hold.expires_at > NOW())
		  )`
	queryArgs := []interface{}{tenant, monthStart, dormantCutoff, weekStart}
	if !parsedCursor.LastMemberAt.IsZero() {
		querySQL += `
		  AND (s.last_member_at > ? OR (s.last_member_at = ? AND s.public_id > ?))`
		queryArgs = append(queryArgs, parsedCursor.LastMemberAt, parsedCursor.LastMemberAt, parsedCursor.StoryID)
	}
	querySQL += `
		ORDER BY s.last_member_at ASC, s.public_id ASC`
	var rows []retentionStoryCandidate
	queryArgs = append(queryArgs, limit+1)
	if err := db.Raw(querySQL+" LIMIT ?", queryArgs...).Scan(&rows).Error; err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}

type retentionStoryBudget struct {
	StoryID uuid.UUID `gorm:"column:story_id"`
	Rows    int       `gorm:"column:rows"`
	Bytes   int64     `gorm:"column:bytes"`
}

func retentionBoundedStoryIDs(db *gorm.DB, tenant, timezone string, policy models.RetentionPolicy) ([]uuid.UUID, error) {
	// Keep this compatibility helper bounded. Preparation uses the richer
	// candidate page below so it can continue past protected/dependency-blocked
	// stories instead of letting them starve later eligible stories.
	probeLimit := 1000
	if policy.MaxRowsPerRun > 0 && policy.MaxRowsPerRun < probeLimit {
		probeLimit = policy.MaxRowsPerRun * 2
		if probeLimit < 100 {
			probeLimit = 100
		}
	}
	candidates, _, err := retentionEligibleStoryPage(db, tenant, timezone, "", probeLimit)
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.StoryID)
	}
	return ids, nil
}

func compactMemberLess(left, right models.ContentItem) bool {
	leftScore, rightScore := engagementScore(left), engagementScore(right)
	if leftScore != rightScore {
		return leftScore > rightScore
	}
	leftTime, rightTime := itemTime(left), itemTime(right)
	if !leftTime.Equal(rightTime) {
		return leftTime.After(rightTime)
	}
	return left.PublicID.String() < right.PublicID.String()
}

func newestMemberLess(left, right models.ContentItem) bool {
	leftTime, rightTime := itemTime(left), itemTime(right)
	if !leftTime.Equal(rightTime) {
		return leftTime.After(rightTime)
	}
	return left.PublicID.String() < right.PublicID.String()
}

func compactSourceKey(item models.ContentItem) string {
	if value := strings.TrimSpace(derefStr(item.SourceName)); value != "" {
		return strings.ToLower(value)
	}
	return strings.ToLower(string(item.Source))
}

func selectCompactMembers(members []models.ContentItem, protected map[uuid.UUID]bool) (lead models.ContentItem, representatives, protectedOnly, retire []models.ContentItem, ok bool) {
	if len(members) == 0 {
		return lead, nil, nil, nil, false
	}
	byScore := append([]models.ContentItem(nil), members...)
	sort.SliceStable(byScore, func(i, j int) bool { return compactMemberLess(byScore[i], byScore[j]) })
	lead = byScore[0]
	selected := map[uuid.UUID]bool{lead.PublicID: true}
	newest := append([]models.ContentItem(nil), members...)
	sort.SliceStable(newest, func(i, j int) bool { return newestMemberLess(newest[i], newest[j]) })
	addRepresentative := func(item models.ContentItem) {
		if len(representatives) >= 3 || selected[item.PublicID] {
			return
		}
		selected[item.PublicID] = true
		representatives = append(representatives, item)
	}
	for _, item := range newest {
		if item.PublicID != lead.PublicID {
			addRepresentative(item)
			break
		}
	}
	sources := map[string]bool{compactSourceKey(lead): true}
	for _, item := range representatives {
		sources[compactSourceKey(item)] = true
	}
	for _, item := range newest {
		if !sources[compactSourceKey(item)] {
			addRepresentative(item)
			sources[compactSourceKey(item)] = true
		}
	}
	for _, item := range byScore {
		addRepresentative(item)
	}
	for _, item := range members {
		if selected[item.PublicID] {
			continue
		}
		if protected[item.PublicID] {
			protectedOnly = append(protectedOnly, item)
		} else {
			retire = append(retire, item)
		}
	}
	return lead, representatives, protectedOnly, retire, true
}

func retentionProtectedContentIDs(db *gorm.DB, tenant string, candidates []uuid.UUID) (map[uuid.UUID]bool, error) {
	protected := make(map[uuid.UUID]bool)
	if len(candidates) == 0 {
		return protected, nil
	}
	var ids []uuid.UUID
	// This is intentionally more conservative than the final 90-day telemetry
	// policy: every existing interaction protects a V1 candidate until the
	// interaction-retention executor and aggregation proof are installed.
	if err := db.Model(&models.UserInteraction{}).Where("content_item_id IN ?", candidates).Pluck("content_item_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("interaction protection query failed: %w", err)
	}
	for _, id := range ids {
		protected[id] = true
	}
	ids = nil
	if err := db.Model(&models.RetentionHold{}).
		Where("tenant_id = ? AND target_type = ? AND target_id IN ? AND released_at IS NULL AND (expires_at IS NULL OR expires_at > ?)", tenant, "content", candidates, time.Now().UTC()).
		Pluck("target_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("retention hold protection query failed: %w", err)
	}
	for _, id := range ids {
		protected[id] = true
	}
	ids = nil
	// A story-level hold protects every member of that story. Recovery target
	// selection uses this same helper, so a held story cannot be purged merely
	// because its individual rows have no direct hold.
	if err := db.Raw(`
		SELECT DISTINCT c.public_id
		FROM content_items c
		JOIN retention_holds h ON h.target_id = c.story_id
		WHERE c.tenant_id = ? AND c.public_id IN ?
		  AND h.tenant_id = ? AND h.target_type = 'story' AND h.released_at IS NULL
		  AND (h.expires_at IS NULL OR h.expires_at > ?)`, tenant, candidates, tenant, time.Now().UTC()).Scan(&ids).Error; err != nil {
		return nil, fmt.Errorf("story hold protection query failed: %w", err)
	}
	for _, id := range ids {
		protected[id] = true
	}
	ids = nil
	if err := db.Model(&models.ModerationReport{}).
		Where("tenant_id = ? AND target_type = ? AND target_id IN ? AND status = ?", tenant, models.ModerationTargetContent, candidates, "open").
		Pluck("target_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("moderation protection query failed: %w", err)
	}
	for _, id := range ids {
		protected[id] = true
	}
	ids = nil
	if err := db.Model(&models.ContentFlag{}).Where("tenant_id = ? AND content_item_id IN ?", tenant, candidates).Pluck("content_item_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("content flag protection query failed: %w", err)
	}
	for _, id := range ids {
		protected[id] = true
	}
	return protected, nil
}

func idsJSON(ids []uuid.UUID) datatypes.JSON {
	values := make([]string, 0, len(ids))
	for _, id := range ids {
		values = append(values, id.String())
	}
	sort.Strings(values)
	raw, _ := json.Marshal(values)
	return datatypes.JSON(raw)
}

func PrepareRetentionCompaction(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var request struct {
		Cursor string `json:"cursor"`
	}
	// An empty body is valid for the first page. A malformed non-empty body is
	// rejected so a caller cannot accidentally restart from an untrusted cursor.
	if c.Request != nil && c.Request.Body != nil && c.Request.ContentLength != 0 {
		if err := c.ShouldBindJSON(&request); err != nil && !errors.Is(err, io.EOF) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid retention cursor request"})
			return
		}
	}
	// A preparation has a fresh pressure sample/run and produces immutable
	// evidence. It cannot execute the plan it prepares.
	run, err := runRetention(db, principal.TenantID, "manual", principal.Email)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errRetentionBusy) {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	policy := loadRetentionPolicy(db, principal.TenantID)
	policy.NewsTimezone = retentionNewsTimezone(db, principal.TenantID)
	candidates, hasMore, err := retentionEligibleStoryPage(db, principal.TenantID, policy.NewsTimezone, request.Cursor, 1000)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(candidates) == 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "no dormant current-month stories are eligible for compaction"})
		return
	}

	var manifest models.RetentionCompactionManifest
	blockedStories := map[string]string{}
	selectedStoryIDs := make([]uuid.UUID, 0, len(candidates))
	var scannedCursor string
	err = db.Transaction(func(tx *gorm.DB) error {
		payload := retentionManifestPayload{TenantID: principal.TenantID, PolicyVersion: policy.PolicyVersion, Timezone: policy.NewsTimezone}
		anchors, protectedIDs, retireIDs := []uuid.UUID{}, []uuid.UUID{}, []uuid.UUID{}
		var selectedBytes int64
		for _, candidate := range candidates {
			priorCursor := scannedCursor
			scannedCursor = encodeRetentionStoryCursor(candidate)
			var story models.Story
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", principal.TenantID, candidate.StoryID).First(&story).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					blockedStories[candidate.StoryID.String()] = "story_changed_during_preparation"
					continue
				}
				return err
			}
			var budget retentionStoryBudget
			if err := tx.Model(&models.ContentItem{}).
				Select("story_id, COUNT(*)::integer AS rows, COALESCE(SUM(pg_column_size(content_items)), 0)::bigint AS bytes").
				Where("tenant_id = ? AND story_id = ? AND type = ? AND status = ? AND COALESCE(news_retention_state, 'full') = 'full'", principal.TenantID, story.PublicID, models.ContentTypeNews, models.ContentStatusReady).
				Group("story_id").Scan(&budget).Error; err != nil {
				return err
			}
			if budget.Rows == 0 {
				blockedStories[story.PublicID.String()] = "no_ready_full_fidelity_members"
				continue
			}
			if budget.Rows > policy.MaxRowsPerRun || budget.Bytes > policy.MaxBytesPerRun {
				blockedStories[story.PublicID.String()] = "story_exceeds_per_run_cap"
				continue
			}
			var members []models.ContentItem
			if err := tx.Where("tenant_id = ? AND story_id = ? AND type = ? AND status = ? AND COALESCE(news_retention_state, 'full') = 'full'", principal.TenantID, story.PublicID, models.ContentTypeNews, models.ContentStatusReady).Order("public_id ASC").Find(&members).Error; err != nil {
				return err
			}
			protected, err := retentionProtectedContentIDs(tx, principal.TenantID, extractPublicIDs(members))
			if err != nil {
				return err
			}
			lead, reps, protectedOnly, retire, selected := selectCompactMembers(members, protected)
			if !selected {
				blockedStories[story.PublicID.String()] = "no_ready_full_fidelity_members"
				continue
			}
			if len(retire) == 0 {
				blockedStories[story.PublicID.String()] = "no_redundant_members"
				continue
			}
			retireIDsForStory := extractPublicIDs(retire)
			issues, preflightErr := retentionDependencyPreflight(tx, principal.TenantID, retireIDsForStory)
			if preflightErr != nil {
				// Query uncertainty is not a candidate-level blocker. It is a
				// fail-closed preparation failure so no partial manifest is built.
				return preflightErr
			}
			if len(issues) > 0 {
				blockedStories[story.PublicID.String()] = retentionDependencyError(issues).Error()
				continue
			}
			var retireBytes int64
			if err := tx.Model(&models.ContentItem{}).Select("COALESCE(SUM(pg_column_size(content_items)), 0)::bigint").Where("tenant_id = ? AND public_id IN ?", principal.TenantID, retireIDsForStory).Scan(&retireBytes).Error; err != nil {
				return err
			}
			if len(retireIDs)+len(retireIDsForStory) > policy.MaxRowsPerRun || selectedBytes+retireBytes > policy.MaxBytesPerRun {
				// Keep the first story that would overflow the batch at the
				// continuation boundary. The next bounded request must be able
				// to select it as the first item instead of silently skipping it.
				scannedCursor = priorCursor
				hasMore = true
				break
			}
			selectedStoryIDs = append(selectedStoryIDs, story.PublicID)
			entry := retentionManifestStory{StoryID: story.PublicID, LeadID: lead.PublicID}
			anchors = append(anchors, lead.PublicID)
			for _, item := range reps {
				entry.RepresentativeIDs = append(entry.RepresentativeIDs, item.PublicID)
				anchors = append(anchors, item.PublicID)
			}
			for _, item := range protectedOnly {
				entry.ProtectedIDs = append(entry.ProtectedIDs, item.PublicID)
				protectedIDs = append(protectedIDs, item.PublicID)
				anchors = append(anchors, item.PublicID)
			}
			for _, item := range retire {
				entry.RetireIDs = append(entry.RetireIDs, item.PublicID)
				retireIDs = append(retireIDs, item.PublicID)
			}
			selectedBytes += retireBytes
			payload.Stories = append(payload.Stories, entry)
		}
		if len(selectedStoryIDs) == 0 {
			if hasMore && scannedCursor != "" {
				return fmt.Errorf("no_eligible_candidates_in_bounded_page; next_cursor=%s", scannedCursor)
			}
			return errors.New("no_eligible_candidates_after_dependency_preflight")
		}
		estimatedBytes := selectedBytes
		payload.BlockedStories = blockedStories
		payload.SelectedCount = len(selectedStoryIDs)
		if scannedCursor != "" {
			payload.NextCursor = scannedCursor
		}
		payload.HasMore = hasMore
		raw, _ := json.Marshal(payload)
		sum := sha256.Sum256(raw)
		hash := hex.EncodeToString(sum[:])
		manifest = models.RetentionCompactionManifest{
			RunID: run.ID, TenantID: principal.TenantID, PolicyVersion: policy.PolicyVersion, Timezone: policy.NewsTimezone,
			ManifestHash: hash, State: "prepared", StoryIDs: idsJSON(selectedStoryIDs), AnchorContentIDs: idsJSON(anchors),
			ProtectedContentIDs: idsJSON(protectedIDs), RetireContentIDs: idsJSON(retireIDs), Evidence: datatypes.JSON(raw),
			StoryCount: len(payload.Stories), AnchorCount: len(anchors), ProtectedCount: len(protectedIDs), RetireCount: len(retireIDs),
			EstimatedBytes: estimatedBytes, ExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		}
		manifest.StoryCount = len(selectedStoryIDs)
		if err := tx.Create(&manifest).Error; err != nil {
			return err
		}
		action := models.RetentionAction{
			RunID: run.ID, TenantID: principal.TenantID, ActionClass: retentionActionPrepareNewsCompaction,
			OwnerSystem: "news_database", TargetScope: "manifest:" + manifest.PublicID.String(), Mode: models.RetentionModeAssist,
			Decision: "approval_required", Outcome: models.RetentionActionApprovalRequired,
			IdempotencyKey: "retention-manifest:" + hash, EvidenceFingerprint: hash, ManifestHash: &hash,
			TargetCount: len(retireIDs), ProtectedCount: len(protectedIDs), EstimatedBytes: estimatedBytes,
			Guardrail: "manifest_only_no_content_mutation", Evidence: datatypes.JSON(raw),
		}
		if err := tx.Create(&action).Error; err != nil {
			return err
		}
		manifest.ActionID = &action.ID
		return tx.Model(&manifest).Update("action_id", action.ID).Error
	})
	if err != nil {
		status := http.StatusConflict
		if strings.Contains(err.Error(), "protection query failed") || strings.Contains(err.Error(), "dependency check") {
			status = http.StatusServiceUnavailable
		}
		response := gin.H{"error": err.Error(), "blocked_stories": blockedStories}
		if strings.Contains(err.Error(), "next_cursor=") {
			response["next_cursor"] = strings.TrimPrefix(strings.SplitN(err.Error(), "next_cursor=", 2)[1], " ")
		}
		c.JSON(status, response)
		return
	}
	retentionAudit(db, principal, "retention.compaction.prepare", manifest.PublicID.String(), "success", map[string]interface{}{"manifest_hash": manifest.ManifestHash, "retire_count": manifest.RetireCount})
	c.JSON(http.StatusCreated, gin.H{"data": manifest})
}
