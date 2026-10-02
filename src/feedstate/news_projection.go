package feedstate

import (
	"encoding/json"
	"errors"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type NewsMetadataScope struct {
	GenerationID uuid.UUID
	MemberHash   string
	Projected    bool
}

// Capture the view before intelligence work leaves CMS. A later head or
// membership change invalidates that work instead of updating another view.
func CaptureNewsMetadataScope(db *gorm.DB, tenant string, storyID uuid.UUID) (NewsMetadataScope, error) {
	var scope NewsMetadataScope
	if !db.Migrator().HasTable(&models.NewsFeedStoryProjection{}) {
		return scope, nil
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane='news'", tenant).First(&head).Error; err != nil {
		return scope, err
	}
	if head.ActiveGenerationID == nil {
		return scope, errors.New("News metadata has no active view")
	}
	scope.GenerationID = *head.ActiveGenerationID
	var projection models.NewsFeedStoryProjection
	err := db.Where("tenant_id=? AND generation_id=? AND story_id=?", tenant, scope.GenerationID, storyID).First(&projection).Error
	if err == nil {
		scope.MemberHash = projection.MemberHash
		scope.Projected = true
		return scope, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return scope, err
	}
	var generation models.FeedGeneration
	if err := db.Where("tenant_id=? AND public_id=?", tenant, scope.GenerationID).First(&generation).Error; err != nil {
		return scope, err
	}
	if generation.Purpose == "content_reset" {
		return scope, errors.New("reset story has no active projection")
	}
	return scope, nil
}

func WriteNewsMetadata(db *gorm.DB, tenant string, storyID uuid.UUID, scope NewsMetadataScope, updates map[string]any) error {
	if len(updates) == 0 {
		return errors.New("News metadata update is empty")
	}
	for key := range updates {
		switch key {
		case "summary", "bullets", "summary_built_at", "category", "related_ids":
		default:
			return errors.New("News metadata field is not an intelligence decoration")
		}
	}
	if scope.Projected && (scope.GenerationID == uuid.Nil || len(scope.MemberHash) != 64) {
		return errors.New("News metadata projection binding is invalid")
	}
	raw, err := json.Marshal(updates)
	if err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if scope.GenerationID != uuid.Nil {
			var head models.FeedGenerationHead
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane='news'", tenant).First(&head).Error; err != nil {
				return err
			}
			if head.ActiveGenerationID == nil || *head.ActiveGenerationID != scope.GenerationID {
				return errors.New("News metadata serving view changed")
			}
		}
		if scope.Projected {
			result := tx.Exec(`UPDATE news_feed_story_projections SET projection=projection || ?::jsonb,updated_at=NOW()
              WHERE tenant_id=? AND generation_id=? AND story_id=? AND member_hash=?`, string(raw), tenant, scope.GenerationID, storyID, scope.MemberHash)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return errors.New("News metadata membership changed")
			}
			return nil
		}
		return tx.Model(&models.Story{}).Where("tenant_id=? AND public_id=?", tenant, storyID).Updates(updates).Error
	})
}

// NewsStoryQuery hydrates metadata through one pinned serving generation.
// PostgreSQL's composite type preserves the Story contract without copying a
// hand-maintained list of columns into every reader.
func NewsStoryQuery(db *gorm.DB, tenant string, generationID uuid.UUID) *gorm.DB {
	if !db.Migrator().HasTable(&models.NewsFeedStoryProjection{}) {
		return db.Model(&models.Story{}).Where("tenant_id=?", tenant)
	}
	view := db.Raw(`SELECT (jsonb_populate_record(NULL::stories,
        to_jsonb(canonical) || COALESCE(projection.projection,'{}'::jsonb))).*
        FROM stories canonical JOIN feed_generations generation ON generation.public_id=? AND generation.tenant_id=canonical.tenant_id AND generation.lane='news'
        LEFT JOIN news_feed_story_projections projection
        ON projection.tenant_id=canonical.tenant_id AND projection.story_id=canonical.public_id AND projection.generation_id=?
        WHERE canonical.tenant_id=? AND (generation.purpose<>'content_reset' OR projection.story_id IS NOT NULL)`, generationID, generationID, tenant)
	return db.Table("(?) AS stories", view)
}

func ActiveNewsStoryQuery(db *gorm.DB, tenant string) *gorm.DB {
	if !db.Migrator().HasTable(&models.NewsFeedStoryProjection{}) {
		return db.Model(&models.Story{}).Where("tenant_id=?", tenant)
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane='news'", tenant).First(&head).Error; err != nil {
		query := db.Model(&models.Story{}).Where("1=0")
		query.AddError(err)
		return query
	}
	if head.ActiveGenerationID == nil {
		query := db.Model(&models.Story{}).Where("1=0")
		query.AddError(errors.New("News serving head has no active generation"))
		return query
	}
	return NewsStoryQuery(db, tenant, *head.ActiveGenerationID).
		Where(`EXISTS (SELECT 1 FROM feed_generation_memberships m WHERE m.generation_id=? AND m.member_type='story' AND m.member_id=stories.public_id)`, *head.ActiveGenerationID)
}

// RebuildNewsStoryProjection uses only the exact generation's deliverable
// members. Removed members cannot influence labels, digests, counts, or kNN.
// Callers hold the lane head before updating projection membership.
func RebuildNewsStoryProjection(tx *gorm.DB, tenant string, generationID, storyID uuid.UUID) error {
	return rebuildNewsProjections(tx, tenant, generationID, &storyID)
}

func RebuildNewsGenerationProjections(tx *gorm.DB, tenant string, generationID uuid.UUID) error {
	return rebuildNewsProjections(tx, tenant, generationID, nil)
}

func rebuildNewsProjections(tx *gorm.DB, tenant string, generationID uuid.UUID, storyID *uuid.UUID) error {
	var invalid int64
	if err := tx.Raw(`SELECT COUNT(*) FROM (
      SELECT c.story_id FROM content_items c JOIN feed_generation_memberships m
        ON m.member_id=c.public_id AND m.generation_id=? AND m.member_type='news_item'
      WHERE c.tenant_id=? AND (?::uuid IS NULL OR c.story_id=?::uuid)
      GROUP BY c.story_id HAVING COUNT(DISTINCT c.embedding_space_id)<>1
        OR COUNT(*)<>COUNT(c.embedding) OR COUNT(*)<>COUNT(NULLIF(c.embedding_space_id,''))
    ) invalid`, generationID, tenant, storyID, storyID).Scan(&invalid).Error; err != nil {
		return err
	}
	if invalid != 0 {
		return errors.New("News projection lacks a consistent embedding space")
	}
	if err := tx.Exec(`WITH members AS (
      SELECT c.* FROM content_items c JOIN feed_generation_memberships m
        ON m.member_id=c.public_id AND m.generation_id=? AND m.member_type='news_item'
      WHERE c.tenant_id=? AND c.type='NEWS' AND c.status='READY' AND c.story_id IS NOT NULL
        AND (?::uuid IS NULL OR c.story_id=?::uuid)
    ), grouped AS (
      SELECT story_id, COUNT(*) AS member_count, AVG(embedding)::text AS centroid,
        MIN(embedding_space_id) AS space_id, MIN(embedding_model) AS model,
        MAX(COALESCE(published_at,created_at)) AS latest,
        (ARRAY_AGG(COALESCE(NULLIF(title,''),'Story') ORDER BY like_count*3+share_count*5+comment_count*2 DESC,public_id))[1] AS label,
        encode(sha256(convert_to(jsonb_agg(jsonb_build_array(public_id,processing_generation,embedding_space_id,encode(sha256(convert_to(embedding::text,'UTF8')),'hex'),title,published_at,status) ORDER BY public_id)::text,'UTF8')),'hex') AS member_hash
      FROM members GROUP BY story_id
    ) INSERT INTO news_feed_story_projections(tenant_id,generation_id,story_id,projection,member_hash,updated_at)
      SELECT ?, ?, g.story_id, to_jsonb(s) || jsonb_build_object(
        'label',g.label,'article_count',g.member_count,'embedding',g.centroid,
        'embedding_space_id',g.space_id,'embedding_model',g.model,'embedding_producer_id',NULL,
        'last_member_at',g.latest,'summary',NULL,'bullets',NULL,'summary_built_at',NULL,
        'category',NULL,'related_ids',NULL,'news_retention_state','full','news_compacted_at',NULL,
        'retained_lead_content_id',NULL,'original_member_count',0,'retained_member_count',0,
        'original_source_count',0,'retained_source_count',0), g.member_hash, NOW()
      FROM grouped g JOIN stories s ON s.tenant_id=? AND s.public_id=g.story_id
      ON CONFLICT(generation_id,story_id) DO UPDATE SET projection=EXCLUDED.projection,
        member_hash=EXCLUDED.member_hash,updated_at=EXCLUDED.updated_at`, generationID, tenant, storyID, storyID, tenant, generationID, tenant).Error; err != nil {
		return err
	}
	if err := tx.Exec(`DELETE FROM news_feed_story_projections p WHERE p.tenant_id=? AND p.generation_id=?
       AND (?::uuid IS NULL OR p.story_id=?::uuid) AND NOT EXISTS (
         SELECT 1 FROM content_items c JOIN feed_generation_memberships m ON m.member_id=c.public_id
         AND m.generation_id=p.generation_id AND m.member_type='news_item'
         WHERE c.tenant_id=p.tenant_id AND c.story_id=p.story_id AND c.type='NEWS' AND c.status='READY'
       )`, tenant, generationID, storyID, storyID).Error; err != nil {
		return err
	}
	return tx.Exec(`DELETE FROM feed_generation_memberships m WHERE m.generation_id=? AND m.member_type='story'
      AND (?::uuid IS NULL OR m.member_id=?::uuid) AND NOT EXISTS (
        SELECT 1 FROM news_feed_story_projections p WHERE p.tenant_id=? AND p.generation_id=m.generation_id AND p.story_id=m.member_id
      )`, generationID, storyID, storyID, tenant).Error
}
