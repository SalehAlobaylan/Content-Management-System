package contentstage

import (
	"sync"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// The stage ledger is additive and is deliberately allowed to be absent while
// a compatibility rollout is in progress. Keep this check centralized so a
// missing migration cannot turn an ordinary ingest into a 500 or make a
// health endpoint claim that durable execution is active.
var stageSchemaCache sync.Map

func SchemaAvailable(db *gorm.DB) bool {
	if db == nil {
		return false
	}
	// Key by the concrete DB handle, not only the dialect. Separate database
	// handles can be at different migration states during rollout and tests.
	key := db
	if value, ok := stageSchemaCache.Load(key); ok {
		return value.(bool)
	}
	available := db.Migrator().HasTable(&models.ContentStageRequest{}) &&
		db.Migrator().HasTable(&models.ContentStageAttempt{}) &&
		db.Migrator().HasTable(&models.ContentStageReceipt{}) &&
		db.Migrator().HasTable(&models.ContentStageEvent{})
	stageSchemaCache.Store(key, available)
	return available
}

// ResetSchemaAvailabilityCache is intended for tests and deliberate operator
// checks after a migration has been applied. Runtime code never calls it.
func ResetSchemaAvailabilityCache() { stageSchemaCache = sync.Map{} }
