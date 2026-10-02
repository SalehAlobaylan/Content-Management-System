// Package feedcontract owns the CMS serving predicates for Pods eligibility
// and tenant-scoped News/Media generation membership. Public reads and
// read-only delivery verification use these predicates so a producer or
// retrieval service cannot claim availability from a weaker duplicated query.
package feedcontract

import (
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	PodsMinDurationSec  = 4*60 + 30
	PodsHardMaxDuration = 40 * 60
	FeedVisibilityShown = "visible"
)

// ServingView is the tenant-scoped pair of lane generations pinned by one
// retrieval request. A rollback generation remains readable only while its
// existing recovery window is open.
type ServingView struct {
	TenantID          string
	NewsGenerationID  uuid.UUID
	MediaGenerationID uuid.UUID
}

// LoadActiveServingView reads both lane heads in one statement so a caller
// cannot construct a mixed tuple across a simultaneous two-lane publication.
func LoadActiveServingView(db *gorm.DB, tenant string) (ServingView, bool, bool) {
	view := ServingView{TenantID: tenant}
	if db == nil || strings.TrimSpace(tenant) == "" || isDryRun(db) ||
		!db.Migrator().HasTable(&models.FeedGenerationHead{}) ||
		!db.Migrator().HasTable(&models.FeedGeneration{}) {
		return view, false, false
	}
	type headRow struct {
		Lane               string
		ActiveGenerationID *uuid.UUID
	}
	var rows []headRow
	if err := db.Model(&models.FeedGenerationHead{}).
		Select("lane, active_generation_id").
		Where("tenant_id = ? AND lane IN ?", tenant, []string{"news", "media"}).
		Find(&rows).Error; err != nil {
		return view, true, false
	}
	for _, row := range rows {
		if row.ActiveGenerationID == nil || *row.ActiveGenerationID == uuid.Nil {
			continue
		}
		switch row.Lane {
		case "news":
			view.NewsGenerationID = *row.ActiveGenerationID
		case "media":
			view.MediaGenerationID = *row.ActiveGenerationID
		}
	}
	return view, true, view.NewsGenerationID != uuid.Nil && view.MediaGenerationID != uuid.Nil
}

// ApplyServingViewMembership constrains retrieval to a pinned, tenant-scoped
// feed view. It allows the previous generation only while its normal rollback
// deadline is open, so an in-flight request remains internally consistent
// across a head switch without making retired generations addressable.
func ApplyServingViewMembership(db *gorm.DB, query *gorm.DB, view ServingView) *gorm.DB {
	if db == nil || query == nil || strings.TrimSpace(view.TenantID) == "" || isDryRun(db) ||
		!db.Migrator().HasTable(&models.FeedGeneration{}) ||
		!db.Migrator().HasTable(&models.FeedGenerationMembership{}) {
		return query.Where("1 = 0")
	}
	query = query.Where("content_items.tenant_id = ?", view.TenantID)
	branches := make([]string, 0, 2)
	args := make([]interface{}, 0, 8)
	now := time.Now().UTC()
	if view.NewsGenerationID != uuid.Nil {
		supported, known := supportsNewsItemMembershipSchema(db)
		if !known {
			return query.Where("1 = 0")
		}
		newsItemClause := "TRUE"
		var newsArgs []interface{}
		if supported {
			newsItemClause = `EXISTS (
				SELECT 1 FROM feed_generation_memberships exact_membership
				WHERE exact_membership.generation_id = ?
				  AND exact_membership.member_type = 'news_item'
				  AND exact_membership.member_id = content_items.public_id
			)`
			newsArgs = append(newsArgs, view.NewsGenerationID)
		}
		branches = append(branches, fmt.Sprintf(`(
			content_items.type = 'NEWS'
			AND EXISTS (
				SELECT 1 FROM feed_generations view_generation
				WHERE view_generation.public_id = ?
				  AND view_generation.tenant_id = content_items.tenant_id
				  AND view_generation.lane = 'news'
				  AND (view_generation.state = 'active' OR
				       (view_generation.state = 'rollback' AND view_generation.rollback_deadline > ?))
			)
			AND EXISTS (
				SELECT 1 FROM feed_generation_memberships story_membership
				WHERE story_membership.generation_id = ?
				  AND story_membership.member_type = 'story'
				  AND story_membership.member_id = content_items.story_id
			)
			AND %s
		)`, newsItemClause))
		args = append(args, view.NewsGenerationID, now, view.NewsGenerationID)
		args = append(args, newsArgs...)
	}
	if view.MediaGenerationID != uuid.Nil {
		branches = append(branches, `(
			content_items.type IN ('VIDEO', 'PODCAST')
			AND EXISTS (
				SELECT 1 FROM feed_generations view_generation
				WHERE view_generation.public_id = ?
				  AND view_generation.tenant_id = content_items.tenant_id
				  AND view_generation.lane = 'media'
				  AND (view_generation.state = 'active' OR
				       (view_generation.state = 'rollback' AND view_generation.rollback_deadline > ?))
			)
			AND EXISTS (
				SELECT 1 FROM feed_generation_memberships media_membership
				WHERE media_membership.generation_id = ?
				  AND media_membership.member_type = 'feed_unit'
				  AND media_membership.member_id = content_items.public_id
			)
		)`)
		args = append(args, view.MediaGenerationID, now, view.MediaGenerationID)
	}
	if len(branches) == 0 {
		return query.Where("1 = 0")
	}
	return query.Where("("+strings.Join(branches, " OR ")+")", args...)
}

// ValidateServingView reports a stable error when the caller supplies a view
// tuple that does not belong to the requested tenant or either lane is absent.
func ValidateServingView(view ServingView) error {
	if strings.TrimSpace(view.TenantID) == "" || view.NewsGenerationID == uuid.Nil || view.MediaGenerationID == uuid.Nil {
		return fmt.Errorf("tenant and both active feed generations are required")
	}
	return nil
}

func SupportsAtomizedPodsSchema(db *gorm.DB) bool {
	return db != nil && db.Migrator().HasColumn(&models.ContentItem{}, "is_feed_unit") &&
		db.Migrator().HasColumn(&models.ContentItem{}, "feed_visibility") &&
		db.Migrator().HasColumn(&models.ContentItem{}, "playback_url")
}

func SupportsStorageStateSchema(db *gorm.DB) bool {
	// Query-shape tests use a DryRun sqlmock without a real information_schema.
	// Runtime reads still check the column before adding the filter.
	return !isDryRun(db) && db != nil && db.Migrator().HasColumn(&models.ContentItem{}, "storage_state")
}

// PodsEligibleMediaQuery is the canonical public serving predicate before
// ranking and per-viewer suppression.
func PodsEligibleMediaQuery(db *gorm.DB, tenantID string, atomizedFeedSchema bool) *gorm.DB {
	storageUnavailableStates := []string{
		models.StorageStateRecoverableDeleted,
		models.StorageStateMissing,
		models.StorageStateRecoveryPending,
		models.StorageStateUnrecoverable,
	}
	if !atomizedFeedSchema {
		q := db.Model(&models.ContentItem{}).
			Where("tenant_id = ?", tenantID).
			Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
			// Pods serving is READY-only. ARCHIVED rows may remain recoverable
			// inventory, but are never a compatibility fallback for a thin feed.
			Where("status = ?", models.ContentStatusReady).
			Where("duration_sec IS NOT NULL AND duration_sec BETWEEN ? AND ?", PodsMinDurationSec, PodsHardMaxDuration).
			// Compatibility rows may have only a direct audio URL or HLS
			// manifest. Format and artwork are presentation metadata, not proof
			// that playback is unavailable.
			Where("media_url IS NOT NULL AND media_url != ''")
		if SupportsStorageStateSchema(db) {
			q = q.Where("(storage_state IS NULL OR storage_state NOT IN ?)", storageUnavailableStates)
		}
		return q
	}

	q := db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("status = ?", models.ContentStatusReady).
		Where("duration_sec IS NOT NULL AND duration_sec BETWEEN ? AND ?", PodsMinDurationSec, PodsHardMaxDuration).
		Where("is_feed_unit = TRUE AND feed_visibility = ?", FeedVisibilityShown).
		// Pods is audio-first. A valid HLS, MP4, or audio-only playback URL is
		// sufficient; a thumbnail must never become a hidden admission gate.
		Where("COALESCE(playback_url, media_url) IS NOT NULL AND COALESCE(playback_url, media_url) != ''")
	if SupportsStorageStateSchema(db) {
		q = q.Where("(storage_state IS NULL OR storage_state NOT IN ?)", storageUnavailableStates)
	}
	// Atomization candidates are not serving generations. This also fences
	// previously marked-visible candidates while completion is reconciled.
	if db.Migrator().HasTable(&models.AtomizationGeneration{}) {
		q = q.Where(`(parent_content_item_id IS NULL OR COALESCE(metadata->>'atomization_generation_id','')='' OR EXISTS (
			SELECT 1 FROM atomization_generations ag WHERE ag.tenant_id=content_items.tenant_id
			AND ag.parent_content_item_id=content_items.parent_content_item_id
			AND ag.public_id::text=content_items.metadata->>'atomization_generation_id' AND ag.state='active'))`)
	}
	return q
}

// ApplyActiveGenerationMembership applies the same serving-generation fence as
// the public feed. It is a no-op during the explicit migration compatibility
// window, which preserves existing public-feed behavior on old schemas.
func ApplyActiveGenerationMembership(db *gorm.DB, query *gorm.DB, tenant, lane, memberType, memberColumn string) *gorm.DB {
	if db == nil || query == nil || isDryRun(db) {
		return query
	}
	_, supported, active := ActiveGeneration(db, tenant, lane)
	if !supported {
		return query
	}
	if !active {
		return query.Where("1 = 0")
	}
	if lane == "news" && memberType == "news_item" {
		supported, known := supportsNewsItemMembershipSchema(db)
		if !known {
			return query.Where("1 = 0")
		}
		if !supported {
			// Only a schema whose check constraint predates exact item membership
			// may use the story-only compatibility projection. An empty generation
			// after migration is a real empty view, not a legacy signal.
			return query.Joins(legacyNewsItemMembershipJoin(memberColumn), tenant, lane, tenant, lane)
		}
		return query.Joins(activeGenerationMembershipJoin(memberColumn), tenant, lane, memberType)
	}
	// Join a derived membership set that exposes only member_id. Joining the
	// generation tables directly leaked their tenant_id into the outer query and
	// made established feed filters such as `tenant_id = ?` ambiguous. Starting
	// from the bounded active membership set keeps the efficient plan without
	// changing the namespace of the caller's content query.
	return query.Joins(activeGenerationMembershipJoin(memberColumn), tenant, lane, memberType)
}

// ApplyPublicItemGenerationMembership keeps UUID-addressable public content
// aligned with the active feed view. It is deliberately correlated by tenant
// because detail and interaction requests know the content ID, not a trusted
// tenant parameter. Story-only News membership is supported only when the
// database still has the pre-exact-item membership constraint.
func ApplyPublicItemGenerationMembership(db *gorm.DB, query *gorm.DB) *gorm.DB {
	if db == nil || query == nil || isDryRun(db) ||
		!db.Migrator().HasTable(&models.FeedGenerationHead{}) ||
		!db.Migrator().HasTable(&models.FeedGenerationMembership{}) {
		return query
	}
	newsItemClause := `EXISTS (
					SELECT 1 FROM feed_generation_heads exact_head
					JOIN feed_generation_memberships exact_membership
					  ON exact_membership.generation_id = exact_head.active_generation_id
					 AND exact_membership.member_type = 'news_item'
					 AND exact_membership.member_id = content_items.public_id
					WHERE exact_head.tenant_id = content_items.tenant_id
					  AND exact_head.lane = 'news'
				)`
	supported, known := supportsNewsItemMembershipSchema(db)
	if !known {
		return query.Where("1 = 0")
	}
	if !supported {
		newsItemClause = "TRUE"
	}
	querySQL := fmt.Sprintf(`(
		(content_items.type IN ? AND EXISTS (
			SELECT 1
			FROM feed_generation_heads media_head
			JOIN feed_generation_memberships media_membership
			  ON media_membership.generation_id = media_head.active_generation_id
			 AND media_membership.member_type = 'feed_unit'
			WHERE media_head.tenant_id = content_items.tenant_id
			  AND media_head.lane = 'media'
			  AND media_membership.member_id = content_items.public_id
		)) OR (
			content_items.type = 'NEWS'
			AND EXISTS (
				SELECT 1
				FROM feed_generation_heads news_head
				JOIN feed_generation_memberships story_membership
				  ON story_membership.generation_id = news_head.active_generation_id
				 AND story_membership.member_type = 'story'
				 AND story_membership.member_id = content_items.story_id
				WHERE news_head.tenant_id = content_items.tenant_id
				  AND news_head.lane = 'news'
			)
			AND %s
		)
	)`, newsItemClause)
	return query.Where(querySQL, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast})
}

func legacyNewsItemMembershipJoin(memberColumn string) string {
	return `JOIN (
        SELECT legacy_content.public_id AS member_id
        FROM feed_generation_heads legacy_head
        JOIN feed_generation_memberships legacy_story
          ON legacy_story.generation_id = legacy_head.active_generation_id
         AND legacy_story.member_type = 'story'
        JOIN content_items legacy_content
          ON legacy_content.tenant_id = legacy_head.tenant_id
         AND legacy_content.story_id = legacy_story.member_id
          AND legacy_content.type = 'NEWS'
        WHERE legacy_head.tenant_id = ?
          AND legacy_head.lane = ?
    ) active_generation_member ON active_generation_member.member_id = ` + memberColumn
}

func activeGenerationMembershipJoin(memberColumn string) string {
	return `JOIN (
        SELECT generation_membership.member_id
        FROM feed_generation_heads generation_head
        JOIN feed_generation_memberships generation_membership
          ON generation_membership.generation_id = generation_head.active_generation_id
        WHERE generation_head.tenant_id = ?
          AND generation_head.lane = ?
          AND generation_membership.member_type = ?
    ) active_generation_member ON active_generation_member.member_id = ` + memberColumn
}

// ActiveGeneration separates the pre-schema compatibility window from a
// missing authority row after generation membership becomes operational.
func ActiveGeneration(db *gorm.DB, tenant, lane string) (uuid.UUID, bool, bool) {
	if db == nil || isDryRun(db) || !db.Migrator().HasTable(&models.FeedGenerationHead{}) || !db.Migrator().HasTable(&models.FeedGenerationMembership{}) {
		return uuid.Nil, false, false
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", tenant, lane).First(&head).Error; err != nil || head.ActiveGenerationID == nil || *head.ActiveGenerationID == uuid.Nil {
		return uuid.Nil, true, false
	}
	return *head.ActiveGenerationID, true, true
}

func isDryRun(db *gorm.DB) bool {
	return db != nil && ((db.Config != nil && db.Config.DryRun) || (db.Statement != nil && db.Statement.DryRun))
}

func supportsNewsItemMembershipSchema(db *gorm.DB) (bool, bool) {
	if db == nil || isDryRun(db) {
		return false, false
	}
	var result struct {
		ConstraintFound bool
		ExactItems      bool
	}
	err := db.Raw(`
		SELECT
			EXISTS (
				SELECT 1 FROM pg_constraint constraint_row
				WHERE constraint_row.conrelid = to_regclass('feed_generation_memberships')
				  AND constraint_row.contype = 'c'
				  AND pg_get_constraintdef(constraint_row.oid) ILIKE '%member_type%'
			) AS constraint_found,
			EXISTS (
				SELECT 1 FROM pg_constraint constraint_row
				WHERE constraint_row.conrelid = to_regclass('feed_generation_memberships')
				  AND constraint_row.contype = 'c'
				  AND pg_get_constraintdef(constraint_row.oid) ILIKE '%member_type%'
				  AND pg_get_constraintdef(constraint_row.oid) ILIKE '%news_item%'
			) AS exact_items
	`).Scan(&result).Error
	return result.ExactItems, err == nil && result.ConstraintFound
}
