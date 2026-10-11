package feedcontract

import (
	"content-management-system/src/models"
	"context"
	"gorm.io/gorm"
)

type readSchemaKey struct{}
type readSchema struct {
	Generations, Atomized, Storage, Atomization, Projection bool
	ExactItems, ConstraintFound                             bool
}

// WithReadSchema batches compatibility checks for one feed read. Only schema
// facts are shared; serving heads, visibility and policy remain live reads.
// Nothing survives the request or a migration/restart.
func WithReadSchema(db *gorm.DB) *gorm.DB {
	if db == nil || isDryRun(db) {
		return db
	}
	if _, ok := requestSchema(db); ok {
		return db
	}
	var schema readSchema
	err := db.Raw(`SELECT
	 to_regclass('feed_generation_heads') IS NOT NULL AND to_regclass('feed_generation_memberships') IS NOT NULL AS generations,
	 (SELECT COUNT(*)=3 FROM pg_attribute WHERE attrelid=to_regclass('content_items') AND attname IN ('is_feed_unit','feed_visibility','playback_url') AND NOT attisdropped) AS atomized,
	 EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('content_items') AND attname='storage_state' AND NOT attisdropped) AS storage,
	 to_regclass('atomization_generations') IS NOT NULL AS atomization,
	 to_regclass('news_feed_story_projections') IS NOT NULL AS projection,
	 EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass('feed_generation_memberships') AND contype='c' AND pg_get_constraintdef(oid) ILIKE '%member_type%') AS constraint_found,
	 EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass('feed_generation_memberships') AND contype='c' AND pg_get_constraintdef(oid) ILIKE '%member_type%' AND pg_get_constraintdef(oid) ILIKE '%news_item%') AS exact_items
	`).Scan(&schema).Error
	if err != nil {
		failed := db.Session(&gorm.Session{})
		failed.AddError(err)
		return failed
	}
	return db.WithContext(context.WithValue(db.Statement.Context, readSchemaKey{}, schema))
}

func requestSchema(db *gorm.DB) (readSchema, bool) {
	if db == nil || db.Statement == nil || db.Statement.Context == nil {
		return readSchema{}, false
	}
	schema, ok := db.Statement.Context.Value(readSchemaKey{}).(readSchema)
	return schema, ok
}

func HasNewsStoryProjection(db *gorm.DB) bool {
	if schema, ok := requestSchema(db); ok {
		return schema.Projection
	}
	return db.Migrator().HasTable(&models.NewsFeedStoryProjection{})
}

func hasGenerationTables(db *gorm.DB) bool {
	if schema, ok := requestSchema(db); ok {
		return schema.Generations
	}
	return db.Migrator().HasTable(&models.FeedGenerationHead{}) && db.Migrator().HasTable(&models.FeedGenerationMembership{})
}

func hasAtomizationGenerations(db *gorm.DB) bool {
	if schema, ok := requestSchema(db); ok {
		return schema.Atomization
	}
	return db.Migrator().HasTable(&models.AtomizationGeneration{})
}
