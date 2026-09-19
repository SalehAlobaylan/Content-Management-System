package contentstage

import (
	"content-management-system/src/models"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const JourneyExpectedGeneration = "journey_expected_generation"

// Called after stage locks, inside the admission transaction. Legacy callers
// without an expected version keep their existing contract.
func CheckJourneyGeneration(tx *gorm.DB, tenant string, id any) error {
	expected, present := tx.Get(JourneyExpectedGeneration)
	if !present {
		return nil
	}
	var item models.ContentItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("public_id", "processing_generation").Where("tenant_id=? AND public_id=?", tenant, id).First(&item).Error; err != nil {
		return err
	}
	version, valid := expected.(int64)
	if !valid || item.ProcessingGeneration != version {
		return fmt.Errorf("Episode generation changed; refresh the journey before acting")
	}
	return nil
}
