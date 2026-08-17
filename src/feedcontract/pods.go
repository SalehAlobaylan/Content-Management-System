// Package feedcontract owns the CMS predicates that define whether a media
// row is currently eligible to be served in Pods. Both public feed assembly
// and read-only delivery verification use this package so a source run cannot
// claim public availability from a weaker, duplicated query.
package feedcontract

import (
	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	PodsMinDurationSec  = 4*60 + 30
	PodsHardMaxDuration = 40 * 60
	FeedVisibilityShown = "visible"
)

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
	// Join a derived membership set that exposes only member_id. Joining the
	// generation tables directly leaked their tenant_id into the outer query and
	// made established feed filters such as `tenant_id = ?` ambiguous. Starting
	// from the bounded active membership set keeps the efficient plan without
	// changing the namespace of the caller's content query.
	return query.Joins(activeGenerationMembershipJoin(memberColumn), tenant, lane, memberType)
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
