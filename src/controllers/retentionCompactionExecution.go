package controllers

// The compaction executor is deliberately small and Assist-only. It applies a
// previously approved immutable manifest in one configured, transactional
// batch; it never discovers targets, widens scope, or touches sources.

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

const retentionActionExecuteNewsCompaction = "news_database.compact_story"

const retentionRecoveryArtifactTTL = 30 * 24 * time.Hour
const retentionRecoveryArtifactMaxBytes = 32 * 1024 * 1024

var (
	errRetentionManifestExpired = errors.New("compaction manifest has expired")
	errRetentionManifestStale   = errors.New("compaction manifest no longer matches live News state")
)

func compactManifestIDs(payload retentionManifestPayload) (storyIDs, anchors, protected, retire []uuid.UUID) {
	for _, story := range payload.Stories {
		storyIDs = append(storyIDs, story.StoryID)
		anchors = append(anchors, story.LeadID)
		anchors = append(anchors, story.RepresentativeIDs...)
		anchors = append(anchors, story.ProtectedIDs...)
		protected = append(protected, story.ProtectedIDs...)
		retire = append(retire, story.RetireIDs...)
	}
	return storyIDs, anchors, protected, retire
}

func uniqueUUIDs(values []uuid.UUID) []uuid.UUID {
	seen := make(map[uuid.UUID]bool, len(values))
	result := make([]uuid.UUID, 0, len(values))
	for _, value := range values {
		if value != uuid.Nil && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func decodeRetentionManifest(manifest models.RetentionCompactionManifest) (retentionManifestPayload, error) {
	var payload retentionManifestPayload
	if err := json.Unmarshal(manifest.Evidence, &payload); err != nil {
		return payload, errors.New("manifest evidence is unreadable")
	}
	canonical, err := json.Marshal(payload)
	if err != nil {
		return payload, err
	}
	sum := sha256.Sum256(canonical)
	if hex.EncodeToString(sum[:]) != manifest.ManifestHash {
		return payload, errors.New("manifest hash verification failed")
	}
	if payload.TenantID != manifest.TenantID || payload.PolicyVersion != manifest.PolicyVersion || payload.Timezone != manifest.Timezone || len(payload.Stories) == 0 {
		return payload, errors.New("manifest scope is invalid")
	}
	return payload, nil
}

func loadApprovedCompaction(tx *gorm.DB, tenant string, actionID uuid.UUID) (models.RetentionAction, models.RetentionCompactionManifest, retentionManifestPayload, error) {
	var action models.RetentionAction
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", tenant, actionID).First(&action).Error; err != nil {
		return action, models.RetentionCompactionManifest{}, retentionManifestPayload{}, err
	}
	var manifest models.RetentionCompactionManifest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND action_id = ?", tenant, action.ID).First(&manifest).Error; err != nil {
		return action, manifest, retentionManifestPayload{}, err
	}
	payload, err := decodeRetentionManifest(manifest)
	return action, manifest, payload, err
}

func retentionActionEvidence(payload map[string]interface{}) datatypes.JSON {
	raw, _ := json.Marshal(payload)
	return datatypes.JSON(raw)
}

func retentionSHA256(parts ...string) string {
	return sha256Hex(strings.Join(parts, "\x00"))
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func retentionTombstoneIdentityForIngest(tenant, idempotencyKey, originalURL string) (string, error) {
	key := strings.TrimSpace(idempotencyKey)
	url := strings.ToLower(strings.TrimSpace(originalURL))
	if key == "" || url == "" {
		return "", errors.New("retirement candidate lacks stable ingest identity")
	}
	return retentionSHA256("news-ingest-v1", tenant, key, url), nil
}

func retentionTombstoneIdentity(tenant string, item models.ContentItem) (identity, source, originalURL string, err error) {
	key := strings.TrimSpace(derefStr(item.IdempotencyKey))
	url := strings.ToLower(strings.TrimSpace(derefStr(item.OriginalURL)))
	identity, err = retentionTombstoneIdentityForIngest(tenant, key, url)
	if err != nil {
		return "", "", "", err
	}
	source = retentionSHA256("news-source-v1", string(item.Source), strings.ToLower(strings.TrimSpace(derefStr(item.SourceName))), strings.ToLower(strings.TrimSpace(derefStr(item.SourceFeedURL))))
	originalURL = retentionSHA256("news-url-v1", url)
	return identity, source, originalURL, nil
}

func createRetentionTombstones(tx *gorm.DB, tenant string, action models.RetentionAction, manifest models.RetentionCompactionManifest, members []models.ContentItem, retireIDs []uuid.UUID) error {
	if len(retireIDs) == 0 {
		return nil
	}
	retireSet := map[uuid.UUID]bool{}
	for _, id := range retireIDs {
		retireSet[id] = true
	}
	rows := make([]models.NewsIngestTombstone, 0, len(retireIDs))
	actionID := action.ID
	for _, item := range members {
		if !retireSet[item.PublicID] {
			continue
		}
		identity, source, originalURL, err := retentionTombstoneIdentity(tenant, item)
		if err != nil {
			return err
		}
		rows = append(rows, models.NewsIngestTombstone{
			TenantID: tenant, IdentityHash: identity, SourceIdentityHash: source, OriginalURLHash: originalURL,
			OriginalContentID: item.PublicID, ManifestHash: manifest.ManifestHash, RetirementActionID: &actionID, Reason: "retention_compaction",
		})
	}
	if len(rows) != len(retireIDs) {
		return errRetentionManifestStale
	}
	if err := tx.Create(&rows).Error; err != nil {
		return fmt.Errorf("create ingest tombstones: %w", err)
	}
	return nil
}

func retentionBatchTargetHash(ids []uuid.UUID) string {
	raw := idsJSON(uniqueUUIDs(ids))
	return sha256Hex(string(raw))
}

// retentionRecoveryPayload deliberately excludes embeddings, media copies and
// source configuration. It is only the minimum logical News record needed to
// diagnose or restore an approved, bounded retirement batch.
type retentionRecoveryPayload struct {
	Version      int                     `json:"version"`
	TenantID     string                  `json:"tenant_id"`
	ManifestHash string                  `json:"manifest_hash"`
	CreatedAt    time.Time               `json:"created_at"`
	Items        []retentionRecoveryItem `json:"items"`
}

type retentionRecoveryItem struct {
	PublicID       uuid.UUID            `json:"id"`
	Type           models.ContentType   `json:"type"`
	Format         *string              `json:"format,omitempty"`
	Source         models.SourceType    `json:"source"`
	Status         models.ContentStatus `json:"status"`
	IdempotencyKey *string              `json:"idempotency_key,omitempty"`
	Title          *string              `json:"title,omitempty"`
	BodyText       *string              `json:"body_text,omitempty"`
	Excerpt        *string              `json:"excerpt,omitempty"`
	Author         *string              `json:"author,omitempty"`
	SourceName     *string              `json:"source_name,omitempty"`
	OriginalURL    *string              `json:"original_url,omitempty"`
	TopicTags      []string             `json:"topic_tags,omitempty"`
	Metadata       datatypes.JSON       `json:"metadata,omitempty"`
	StoryID        *uuid.UUID           `json:"story_id,omitempty"`
	PublishedAt    *time.Time           `json:"published_at,omitempty"`
	CreatedAt      time.Time            `json:"created_at"`
}

func prepareRecoveryArtifact(tenant string, action models.RetentionAction, manifest models.RetentionCompactionManifest, members []models.ContentItem, retireIDs []uuid.UUID, now time.Time) (models.RetentionRecoveryArtifact, error) {
	retireSet := map[uuid.UUID]bool{}
	for _, id := range retireIDs {
		retireSet[id] = true
	}
	sort.Slice(members, func(i, j int) bool { return members[i].PublicID.String() < members[j].PublicID.String() })
	// Manifest creation time, rather than execution time, keeps a retry's
	// checksum/key stable if the database transaction rolls back after upload.
	payload := retentionRecoveryPayload{Version: 1, TenantID: tenant, ManifestHash: manifest.ManifestHash, CreatedAt: manifest.CreatedAt}
	for _, item := range members {
		if !retireSet[item.PublicID] {
			continue
		}
		payload.Items = append(payload.Items, retentionRecoveryItem{PublicID: item.PublicID, Type: item.Type, Format: item.Format, Source: item.Source, Status: item.Status, IdempotencyKey: item.IdempotencyKey, Title: item.Title, BodyText: item.BodyText, Excerpt: item.Excerpt, Author: item.Author, SourceName: item.SourceName, OriginalURL: item.OriginalURL, TopicTags: []string(item.TopicTags), Metadata: item.Metadata, StoryID: item.StoryID, PublishedAt: item.PublishedAt, CreatedAt: item.CreatedAt})
	}
	if len(payload.Items) != len(retireIDs) {
		return models.RetentionRecoveryArtifact{}, errRetentionManifestStale
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return models.RetentionRecoveryArtifact{}, fmt.Errorf("encode recovery artifact: %w", err)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err = writer.Write(raw); err != nil {
		return models.RetentionRecoveryArtifact{}, err
	}
	if err = writer.Close(); err != nil {
		return models.RetentionRecoveryArtifact{}, err
	}
	if compressed.Len() > retentionRecoveryArtifactMaxBytes {
		return models.RetentionRecoveryArtifact{}, errors.New("recovery artifact exceeds the 32 MiB storage preflight cap")
	}
	sum := sha256.Sum256(compressed.Bytes())
	checksum := hex.EncodeToString(sum[:])
	key := fmt.Sprintf("system/recovery/%s/%s/%s.json.gz", tenant, manifest.PublicID.String(), checksum)
	request := map[string]string{"key": key, "sha256": checksum, "payload_base64": base64.StdEncoding.EncodeToString(compressed.Bytes())}
	_, status, err := callAggregationInternal(http.MethodPost, "/internal/recovery-artifacts", request)
	if err != nil {
		return models.RetentionRecoveryArtifact{}, fmt.Errorf("recovery storage preflight: %w", err)
	}
	if status < 200 || status >= 300 {
		return models.RetentionRecoveryArtifact{}, fmt.Errorf("recovery storage preflight returned %d", status)
	}
	_, verifyStatus, err := callAggregationInternal(http.MethodPost, "/internal/recovery-artifacts/verify", map[string]string{"key": key, "sha256": checksum})
	if err != nil || verifyStatus < 200 || verifyStatus >= 300 {
		return models.RetentionRecoveryArtifact{}, fmt.Errorf("recovery artifact readback verification failed")
	}
	expiresAt := now.Add(retentionRecoveryArtifactTTL)
	return models.RetentionRecoveryArtifact{ActionID: action.ID, ManifestID: manifest.ID, TenantID: tenant, ArtifactKey: key, SHA256: checksum, CompressedBytes: int64(compressed.Len()), UncompressedBytes: int64(len(raw)), State: "verified", ExpiresAt: expiresAt, VerifiedAt: &now}, nil
}

func deleteRecoveryArtifact(key string) error {
	if strings.TrimSpace(key) == "" {
		return nil
	}
	_, status, err := callAggregationInternal(http.MethodDelete, "/internal/recovery-artifacts", map[string]string{"key": key})
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("recovery artifact delete returned %d", status)
	}
	return nil
}

func markRetentionCompactionBlocked(db *gorm.DB, action models.RetentionAction, manifest models.RetentionCompactionManifest, cause error) {
	now := time.Now().UTC()
	evidence := retentionActionEvidence(map[string]interface{}{"phase": "preflight", "error": cause.Error(), "manifest_hash": manifest.ManifestHash})
	_ = db.Transaction(func(tx *gorm.DB) error {
		_ = tx.Model(&models.RetentionAction{}).Where("id = ?", action.ID).Updates(map[string]interface{}{
			"outcome": models.RetentionActionToolFailed, "finished_at": now, "guardrail": "execution_preflight_blocked", "verification": evidence,
		}).Error
		return tx.Model(&models.RetentionCompactionManifest{}).Where("id = ?", manifest.ID).Updates(map[string]interface{}{"state": "blocked", "updated_at": now}).Error
	})
}

func revalidateCompactionPlan(tx *gorm.DB, tenant string, payload retentionManifestPayload, policy models.RetentionPolicy) ([]models.Story, []models.ContentItem, error) {
	storyIDs, _, _, retireIDs := compactManifestIDs(payload)
	storyIDs, retireIDs = uniqueUUIDs(storyIDs), uniqueUUIDs(retireIDs)
	if len(storyIDs) == 0 || len(storyIDs) != len(payload.Stories) {
		return nil, nil, errRetentionManifestStale
	}
	var stories []models.Story
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id IN ?", tenant, storyIDs).Order("public_id ASC").Find(&stories).Error; err != nil || len(stories) != len(storyIDs) {
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, errRetentionManifestStale
	}
	var members []models.ContentItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("tenant_id = ? AND story_id IN ? AND type = ? AND status = ?", tenant, storyIDs, models.ContentTypeNews, models.ContentStatusReady).
		Find(&members).Error; err != nil {
		return nil, nil, err
	}
	_, anchors, _, expectedRetire := compactManifestIDs(payload)
	expected := uniqueUUIDs(append(anchors, expectedRetire...))
	actual := uniqueUUIDs(extractPublicIDs(members))
	if len(expected) != len(actual) {
		return nil, nil, errRetentionManifestStale
	}
	actualSet := make(map[uuid.UUID]bool, len(actual))
	for _, id := range actual {
		actualSet[id] = true
	}
	for _, id := range expected {
		if !actualSet[id] {
			return nil, nil, errRetentionManifestStale
		}
	}
	for _, item := range members {
		if derefStr(item.NewsRetentionState) != "full" || derefStr(item.NewsFeedRole) != "full_member" {
			return nil, nil, errRetentionManifestStale
		}
	}
	if len(members) > policy.MaxRowsPerRun {
		return nil, nil, errors.New("live member count exceeds the configured row cap")
	}
	if len(retireIDs) > 0 {
		var retireBytes int64
		if err := tx.Raw("SELECT COALESCE(SUM(pg_column_size(content_items)), 0)::bigint FROM content_items WHERE tenant_id = ? AND public_id IN ?", tenant, retireIDs).Scan(&retireBytes).Error; err != nil {
			return nil, nil, err
		}
		if retireBytes > policy.MaxBytesPerRun {
			return nil, nil, errors.New("live retirement bytes exceed the configured byte cap")
		}
	}
	protected := retentionProtectedContentIDs(tx, tenant, actual)
	for _, id := range retireIDs {
		if protected[id] {
			return nil, nil, errors.New("a retirement candidate gained a protected reference")
		}
	}
	issues, err := retentionDependencyPreflight(tx, tenant, retireIDs)
	if err != nil {
		return nil, nil, err
	}
	if err := retentionDependencyError(issues); err != nil {
		return nil, nil, err
	}
	return stories, members, nil
}

func compactAnchorUpdates(tx *gorm.DB, tenant, hash string, payload retentionManifestPayload, now time.Time) error {
	for _, story := range payload.Stories {
		if err := tx.Model(&models.ContentItem{}).Where("tenant_id = ? AND public_id = ?", tenant, story.LeadID).Updates(map[string]interface{}{
			"news_retention_state": "compact", "news_feed_role": "lead", "news_representative_ordinal": nil,
			"news_compacted_at": now, "news_compaction_hash": hash,
			"embedding": nil, "embedding_model": nil, "embedding_space_id": nil, "embedding_producer_id": nil,
			"image_embedding": nil, "image_embedding_model": nil, "image_embedding_space_id": nil, "image_embedding_producer_id": nil,
		}).Error; err != nil {
			return err
		}
		for index, id := range story.RepresentativeIDs {
			ordinal := int16(index + 1)
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id = ? AND public_id = ?", tenant, id).Updates(map[string]interface{}{
				"news_retention_state": "compact", "news_feed_role": "representative", "news_representative_ordinal": ordinal,
				"news_compacted_at": now, "news_compaction_hash": hash,
				"embedding": nil, "embedding_model": nil, "embedding_space_id": nil, "embedding_producer_id": nil,
				"image_embedding": nil, "image_embedding_model": nil, "image_embedding_space_id": nil, "image_embedding_producer_id": nil,
			}).Error; err != nil {
				return err
			}
		}
		if len(story.ProtectedIDs) > 0 {
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id = ? AND public_id IN ?", tenant, story.ProtectedIDs).Updates(map[string]interface{}{
				"news_retention_state": "compact", "news_feed_role": "protected_only", "news_representative_ordinal": nil,
				"news_compacted_at": now, "news_compaction_hash": hash,
				"embedding": nil, "embedding_model": nil, "embedding_space_id": nil, "embedding_producer_id": nil,
				"image_embedding": nil, "image_embedding_model": nil, "image_embedding_space_id": nil, "image_embedding_producer_id": nil,
			}).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

func updateCompactedStories(tx *gorm.DB, tenant string, payload retentionManifestPayload, members []models.ContentItem, now time.Time) error {
	byStory := make(map[uuid.UUID][]models.ContentItem)
	for _, member := range members {
		if member.StoryID != nil {
			byStory[*member.StoryID] = append(byStory[*member.StoryID], member)
		}
	}
	for _, plan := range payload.Stories {
		retained := append([]uuid.UUID{plan.LeadID}, plan.RepresentativeIDs...)
		retained = append(retained, plan.ProtectedIDs...)
		retainedSet := make(map[uuid.UUID]bool, len(retained))
		for _, id := range retained {
			retainedSet[id] = true
		}
		sources := map[string]bool{}
		for _, member := range byStory[plan.StoryID] {
			if retainedSet[member.PublicID] {
				sources[compactSourceKey(member)] = true
			}
		}
		if err := tx.Model(&models.Story{}).Where("tenant_id = ? AND public_id = ?", tenant, plan.StoryID).Updates(map[string]interface{}{
			"news_retention_state": "compact", "news_compacted_at": now, "retained_lead_content_id": plan.LeadID,
			"original_member_count": len(byStory[plan.StoryID]), "retained_member_count": len(retained),
			"original_source_count": len(uniqueCompactSources(byStory[plan.StoryID])), "retained_source_count": len(sources),
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

func uniqueCompactSources(items []models.ContentItem) map[string]bool {
	result := map[string]bool{}
	for _, item := range items {
		result[compactSourceKey(item)] = true
	}
	return result
}

func advanceNewsSnapshotGenerations(tx *gorm.DB, tenantID string) error {
	for _, window := range []string{models.NewsWindowToday, models.NewsWindowWeek, models.NewsWindowMonth} {
		if err := tx.Exec(`INSERT INTO news_snapshot_generations (tenant_id, "window", generation, updated_at)
            VALUES (?, ?, 2, NOW())
            ON CONFLICT (tenant_id, "window") DO UPDATE
            SET generation = news_snapshot_generations.generation + 1, updated_at = NOW()`, tenantID, window).Error; err != nil {
			return err
		}
		if err := tx.Where("tenant_id = ? AND \"window\" = ?", tenantID, window).Delete(&models.NewsSnapshot{}).Error; err != nil {
			return err
		}
	}
	return nil
}

func recordCompactionMonthlyRollup(tx *gorm.DB, tenant, timezone string, now time.Time) error {
	if err := ensureCurrentRetentionMonth(tx, tenant, timezone, now); err != nil {
		return err
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		location = time.FixedZone("Asia/Riyadh", 3*60*60)
	}
	local := now.In(location)
	monthStart := time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, time.UTC)
	var stories, retained int64
	if err := tx.Model(&models.Story{}).Where("tenant_id = ? AND news_retention_state = ? AND news_compacted_at >= ?", tenant, "compact", monthStart).Count(&stories).Error; err != nil {
		return err
	}
	if err := tx.Model(&models.ContentItem{}).Where("tenant_id = ? AND news_retention_state = ? AND news_compacted_at >= ?", tenant, "compact", monthStart).Count(&retained).Error; err != nil {
		return err
	}
	return tx.Model(&models.RetentionMonth{}).Where("tenant_id = ? AND month_start = ?", tenant, monthStart).Updates(map[string]interface{}{"state": "compacting", "state_reason": "bounded compaction executed", "compacted_story_count": stories, "retained_content_count": retained}).Error
}

func runApprovedCompactionMutation(db *gorm.DB, tenant string, actionID uuid.UUID, policy models.RetentionPolicy) (models.RetentionAction, models.RetentionCompactionManifest, retentionManifestPayload, error) {
	var action models.RetentionAction
	var manifest models.RetentionCompactionManifest
	var payload retentionManifestPayload
	var externalArtifactKey string
	now := time.Now().UTC()
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		action, manifest, payload, err = loadApprovedCompaction(tx, tenant, actionID)
		if err != nil {
			return err
		}
		if action.ActionClass != retentionActionPrepareNewsCompaction || action.Outcome != models.RetentionActionApproved || manifest.State != "approved" {
			return errors.New("approved compaction action is required")
		}
		if now.After(manifest.ExpiresAt) {
			return errRetentionManifestExpired
		}
		if action.ManifestHash == nil || *action.ManifestHash != manifest.ManifestHash {
			return errors.New("action and manifest hash do not match")
		}
		var members []models.ContentItem
		_, members, err = revalidateCompactionPlan(tx, tenant, payload, policy)
		if err != nil {
			return err
		}
		_, _, _, retireIDs := compactManifestIDs(payload)
		retireIDs = uniqueUUIDs(retireIDs)
		batch := models.RetentionCompactionBatch{
			ActionID: action.ID, ManifestID: manifest.ID, TenantID: tenant, BatchIndex: 0, State: "running",
			TargetHash: retentionBatchTargetHash(retireIDs), TargetIDs: idsJSON(retireIDs), TargetCount: len(retireIDs),
			EstimatedBytes: manifest.EstimatedBytes, StartedAt: &now,
			BeforeEvidence: retentionActionEvidence(map[string]interface{}{"manifest_hash": manifest.ManifestHash, "target_count": len(retireIDs)}),
		}
		if err := tx.Create(&batch).Error; err != nil {
			return fmt.Errorf("create compaction batch: %w", err)
		}
		if len(retireIDs) > 0 {
			artifact, err := prepareRecoveryArtifact(tenant, action, manifest, members, retireIDs, now)
			if err != nil {
				return err
			}
			externalArtifactKey = artifact.ArtifactKey
			if err := tx.Create(&artifact).Error; err != nil {
				return fmt.Errorf("record recovery artifact: %w", err)
			}
			if err := tx.Model(&models.RetentionCompactionBatch{}).Where("id = ?", batch.ID).Update("before_evidence", retentionActionEvidence(map[string]interface{}{"manifest_hash": manifest.ManifestHash, "target_count": len(retireIDs), "recovery_ref": artifact.ArtifactKey, "recovery_sha256": artifact.SHA256, "recovery_expires_at": artifact.ExpiresAt})).Error; err != nil {
				return err
			}
		}
		if err := createRetentionTombstones(tx, tenant, action, manifest, members, retireIDs); err != nil {
			return err
		}
		if err := compactAnchorUpdates(tx, tenant, manifest.ManifestHash, payload, now); err != nil {
			return err
		}
		if err := updateCompactedStories(tx, tenant, payload, members, now); err != nil {
			return err
		}
		if len(retireIDs) > 0 {
			result := tx.Where("tenant_id = ? AND public_id IN ? AND type = ? AND news_retention_state = ?", tenant, retireIDs, models.ContentTypeNews, "full").Delete(&models.ContentItem{})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != int64(len(retireIDs)) {
				return errRetentionManifestStale
			}
		}
		if err := advanceNewsSnapshotGenerations(tx, tenant); err != nil {
			return err
		}
		if err := recordCompactionMonthlyRollup(tx, tenant, policy.NewsTimezone, now); err != nil {
			return err
		}
		if err := tx.Model(&models.RetentionCompactionManifest{}).Where("id = ?", manifest.ID).Updates(map[string]interface{}{"state": "executed", "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.RetentionCompactionBatch{}).Where("id = ?", batch.ID).Updates(map[string]interface{}{
			"state": "tool_succeeded", "finished_at": now,
			"after_evidence": retentionActionEvidence(map[string]interface{}{"tombstones_created": len(retireIDs), "snapshot_generation_advanced": true}),
		}).Error; err != nil {
			return err
		}
		return tx.Model(&models.RetentionAction{}).Where("id = ?", action.ID).Updates(map[string]interface{}{
			"action_class": retentionActionExecuteNewsCompaction, "outcome": models.RetentionActionToolSucceeded, "started_at": now, "finished_at": now,
			"guardrail": "human_approved_exact_manifest_recovery_gated_bounded_transaction", "claim_token": nil, "claim_expires_at": nil, "recovery_ref": externalArtifactKey,
		}).Error
	})
	if err != nil && externalArtifactKey != "" {
		_ = deleteRecoveryArtifact(externalArtifactKey)
	}
	if err == nil {
		evictLocalNewsSnapshots(tenant)
		action.ActionClass = retentionActionExecuteNewsCompaction
		action.Outcome = models.RetentionActionToolSucceeded
		manifest.State = "executed"
	}
	return action, manifest, payload, err
}

func snapshotContainsRetiredID(snapshot models.NewsSnapshot, retired map[uuid.UUID]bool) (uuid.UUID, bool) {
	var slides []StorySlide
	if json.Unmarshal(snapshot.Slides, &slides) != nil {
		return uuid.Nil, true
	}
	for _, slide := range slides {
		if retired[slide.Featured.LeadID] {
			return slide.Featured.LeadID, true
		}
		for _, member := range slide.Featured.Members {
			if retired[member.ID] {
				return member.ID, true
			}
		}
		for _, related := range slide.Related {
			if retired[related.LeadID] {
				return related.LeadID, true
			}
		}
	}
	return uuid.Nil, false
}

func verifyCompactionReadback(db *gorm.DB, tenant string, manifest models.RetentionCompactionManifest, payload retentionManifestPayload) (datatypes.JSON, error) {
	_, anchors, protected, retireIDs := compactManifestIDs(payload)
	retired := map[uuid.UUID]bool{}
	for _, id := range retireIDs {
		retired[id] = true
	}
	result := map[string]interface{}{"manifest_hash": manifest.ManifestHash, "retire_count": len(retired), "snapshots": map[string]int{}}
	for _, window := range []string{models.NewsWindowToday, models.NewsWindowWeek, models.NewsWindowMonth} {
		count, err := buildNewsSnapshot(db, tenant, window)
		if err != nil {
			return nil, fmt.Errorf("rebuild %s snapshot: %w", window, err)
		}
		result["snapshots"].(map[string]int)[window] = count
		var snapshot models.NewsSnapshot
		if err := db.Where("tenant_id = ? AND \"window\" = ?", tenant, window).First(&snapshot).Error; err != nil {
			return nil, fmt.Errorf("read back %s snapshot: %w", window, err)
		}
		if id, found := snapshotContainsRetiredID(snapshot, retired); found {
			return nil, fmt.Errorf("%s snapshot references retired content %s", window, id)
		}
	}
	storyIDs, _, _, _ := compactManifestIDs(payload)
	var remaining int64
	if err := db.Model(&models.ContentItem{}).Where("tenant_id = ? AND story_id IN ? AND type = ? AND COALESCE(news_retention_state, 'full') = 'full'", tenant, uniqueUUIDs(storyIDs), models.ContentTypeNews).Count(&remaining).Error; err != nil {
		return nil, err
	}
	if remaining != 0 {
		return nil, fmt.Errorf("%d full-fidelity rows remain in compacted stories", remaining)
	}
	var liveRetirees int64
	if len(retireIDs) > 0 {
		if err := db.Model(&models.ContentItem{}).Where("tenant_id = ? AND public_id IN ?", tenant, retireIDs).Count(&liveRetirees).Error; err != nil {
			return nil, err
		}
	}
	if liveRetirees != 0 {
		return nil, fmt.Errorf("%d selected retire rows remain", liveRetirees)
	}
	retainedIDs := uniqueUUIDs(append(anchors, protected...))
	var readableAnchors int64
	if err := db.Model(&models.ContentItem{}).Where("tenant_id = ? AND public_id IN ? AND type = ? AND status = ? AND story_id IS NOT NULL", tenant, retainedIDs, models.ContentTypeNews, models.ContentStatusReady).Count(&readableAnchors).Error; err != nil {
		return nil, err
	}
	if readableAnchors != int64(len(retainedIDs)) {
		return nil, fmt.Errorf("%d retained News detail records are not readable", int64(len(retainedIDs))-readableAnchors)
	}
	var retiredInteractions int64
	if len(retireIDs) > 0 {
		if err := db.Model(&models.UserInteraction{}).Where("content_item_id IN ?", retireIDs).Count(&retiredInteractions).Error; err != nil {
			return nil, err
		}
	}
	if retiredInteractions != 0 {
		return nil, fmt.Errorf("%d interactions still reference retired content", retiredInteractions)
	}
	result["retained_detail_count"] = readableAnchors
	result["retired_interaction_count"] = retiredInteractions
	var retentionRun models.RetentionRun
	if err := db.Where("id = ? AND tenant_id = ?", manifest.RunID, tenant).First(&retentionRun).Error; err != nil {
		return nil, fmt.Errorf("load correlated retention run: %w", err)
	}
	feedRun, err := runFeedIntegrity(db, tenant, feedIntegrityRunOptions{
		Trigger:   "retention:" + retentionRun.CorrelationID.String(),
		CreatedBy: "retention-autopilot",
		Tier:      models.FeedIntegrityTierDeep,
	})
	if err != nil || feedRun.Status != models.FeedIntegrityRunCompleted {
		if err != nil {
			return nil, fmt.Errorf("feed integrity verification: %w", err)
		}
		return nil, fmt.Errorf("feed integrity verification finished %s", feedRun.Status)
	}
	health, anomalies := collectSystemHealthSnapshot(db)
	if health.Overall == "unhealthy" {
		return nil, errors.New("system health verification is unhealthy")
	}
	result["feed_integrity"] = map[string]interface{}{
		"run_id": feedRun.PublicID.String(), "status": feedRun.Status, "headline": feedRun.Headline,
	}
	result["system_health"] = map[string]interface{}{
		"overall": health.Overall, "anomaly_count": len(anomalies), "observed_at": health.Timestamp,
	}
	return retentionActionEvidence(result), nil
}

func finalizeCompactionVerification(db *gorm.DB, tenant string, action models.RetentionAction, manifest models.RetentionCompactionManifest, payload retentionManifestPayload) (models.RetentionAction, error) {
	now := time.Now().UTC()
	_ = db.Model(&models.RetentionAction{}).Where("id = ?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerifying, "started_at": now}).Error
	_ = db.Model(&models.RetentionCompactionBatch{}).Where("action_id = ? AND tenant_id = ? AND batch_index = 0", action.ID, tenant).Update("state", "verifying").Error
	verification, err := verifyCompactionReadback(db, tenant, manifest, payload)
	if err != nil {
		_ = db.Model(&models.RetentionAction{}).Where("id = ?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerifyFailed, "finished_at": time.Now().UTC(), "verification": retentionActionEvidence(map[string]interface{}{"phase": "readback", "error": err.Error()})}).Error
		_ = db.Model(&models.RetentionCompactionBatch{}).Where("action_id = ? AND tenant_id = ? AND batch_index = 0", action.ID, tenant).Updates(map[string]interface{}{"state": "verification_failed", "error": err.Error(), "finished_at": time.Now().UTC()}).Error
		return action, err
	}
	if err := db.Exec("VACUUM (ANALYZE) content_items").Error; err != nil {
		maintenanceErr := fmt.Errorf("post-compaction vacuum/analyze: %w", err)
		_ = db.Model(&models.RetentionAction{}).Where("id = ?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerifyFailed, "finished_at": time.Now().UTC(), "verification": retentionActionEvidence(map[string]interface{}{"phase": "maintenance", "error": maintenanceErr.Error()})}).Error
		_ = db.Model(&models.RetentionCompactionBatch{}).Where("action_id = ? AND tenant_id = ? AND batch_index = 0", action.ID, tenant).Updates(map[string]interface{}{"state": "verification_failed", "error": maintenanceErr.Error(), "finished_at": time.Now().UTC()}).Error
		return action, maintenanceErr
	}
	afterSample, err := collectRetentionDBSample(db, tenant)
	if err != nil {
		measurementErr := fmt.Errorf("post-compaction database measurement: %w", err)
		_ = db.Model(&models.RetentionAction{}).Where("id = ?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerifyFailed, "finished_at": time.Now().UTC(), "verification": retentionActionEvidence(map[string]interface{}{"phase": "measurement", "error": measurementErr.Error()})}).Error
		_ = db.Model(&models.RetentionCompactionBatch{}).Where("action_id = ? AND tenant_id = ? AND batch_index = 0", action.ID, tenant).Updates(map[string]interface{}{"state": "verification_failed", "error": measurementErr.Error(), "finished_at": time.Now().UTC()}).Error
		return action, measurementErr
	}
	var verificationMap map[string]interface{}
	_ = json.Unmarshal(verification, &verificationMap)
	verificationMap["maintenance"] = "VACUUM (ANALYZE) content_items"
	verificationMap["after_database_bytes"] = afterSample.DatabaseBytes
	verification = retentionActionEvidence(verificationMap)
	if err := db.Model(&models.RetentionAction{}).Where("id = ?", action.ID).Updates(map[string]interface{}{"outcome": models.RetentionActionVerified, "finished_at": time.Now().UTC(), "verification": verification, "after_bytes": afterSample.DatabaseBytes}).Error; err != nil {
		return action, err
	}
	_ = db.Model(&models.RetentionCompactionBatch{}).Where("action_id = ? AND tenant_id = ? AND batch_index = 0", action.ID, tenant).Updates(map[string]interface{}{"state": "verification_passed", "finished_at": time.Now().UTC(), "after_evidence": verification}).Error
	if err := db.Where("id = ?", action.ID).First(&action).Error; err != nil {
		return action, err
	}
	return action, nil
}

// ExecuteRetentionCompaction is a human-operated Assist action. It accepts no
// targets from the request: the approved manifest is the sole authority.
func ExecuteRetentionCompaction(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	actionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid action id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadRetentionPolicy(db, principal.TenantID)

	var existing models.RetentionAction
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, actionID).First(&existing).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "retention action not found"})
		return
	}
	var manifest models.RetentionCompactionManifest
	if err := db.Where("tenant_id = ? AND action_id = ?", principal.TenantID, existing.ID).First(&manifest).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "action has no compaction manifest"})
		return
	}
	payload, err := decodeRetentionManifest(manifest)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	if existing.Outcome == models.RetentionActionToolSucceeded || existing.Outcome == models.RetentionActionVerifying || existing.Outcome == models.RetentionActionVerifyFailed {
		verified, verifyErr := finalizeCompactionVerification(db, principal.TenantID, existing, manifest, payload)
		if verifyErr != nil {
			c.JSON(http.StatusConflict, gin.H{"error": verifyErr.Error(), "data": verified})
			return
		}
		retentionAudit(db, principal, "retention.compaction.verify", manifest.PublicID.String(), "success", map[string]interface{}{"manifest_hash": manifest.ManifestHash})
		c.JSON(http.StatusOK, gin.H{"data": verified})
		return
	}
	if !policy.Enabled || policy.Mode != models.RetentionModeAssist {
		c.JSON(http.StatusConflict, gin.H{"error": "compaction execution requires an enabled Assist-mode retention policy"})
		return
	}
	beforeSample, sampleErr := collectRetentionDBSample(db, principal.TenantID)
	if sampleErr != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "pre-compaction database measurement: " + sampleErr.Error()})
		return
	}
	if err := db.Model(&models.RetentionAction{}).Where("id = ?", existing.ID).Update("before_bytes", beforeSample.DatabaseBytes).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "record pre-compaction measurement: " + err.Error()})
		return
	}

	action, executedManifest, executedPayload, execErr := runApprovedCompactionMutation(db, principal.TenantID, actionID, policy)
	if execErr != nil {
		if action.ID != 0 && executedManifest.ID != 0 {
			if errors.Is(execErr, errRetentionManifestExpired) {
				_ = db.Model(&models.RetentionAction{}).Where("id = ?", action.ID).Update("outcome", models.RetentionActionExpired).Error
				_ = db.Model(&models.RetentionCompactionManifest{}).Where("id = ?", executedManifest.ID).Update("state", "expired").Error
			} else {
				markRetentionCompactionBlocked(db, action, executedManifest, execErr)
			}
		}
		c.JSON(http.StatusConflict, gin.H{"error": execErr.Error()})
		return
	}
	verified, verifyErr := finalizeCompactionVerification(db, principal.TenantID, action, executedManifest, executedPayload)
	if verifyErr != nil {
		c.JSON(http.StatusConflict, gin.H{"error": verifyErr.Error(), "data": verified})
		return
	}
	retentionAudit(db, principal, "retention.compaction.execute", executedManifest.PublicID.String(), "success", map[string]interface{}{"manifest_hash": executedManifest.ManifestHash, "action_id": verified.PublicID.String()})
	c.JSON(http.StatusOK, gin.H{"data": verified})
}

// sortedManifestStoryIDs is intentionally retained as a pure helper for
// deterministic test fixtures and operator evidence display.
func sortedManifestStoryIDs(payload retentionManifestPayload) []string {
	ids, _, _, _ := compactManifestIDs(payload)
	values := make([]string, 0, len(ids))
	for _, id := range uniqueUUIDs(ids) {
		values = append(values, id.String())
	}
	sort.Strings(values)
	return values
}
