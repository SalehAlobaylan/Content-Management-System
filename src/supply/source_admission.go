package supply

import (
	"fmt"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Normal admission keeps every active native request as a source-slot blocker.
func CountSourceAdmissionBlockers(tx *gorm.DB, tenant string, source uuid.UUID) (int64, error) {
	var count int64
	err := tx.Model(&models.SourceRunRequest{}).Where("tenant_id=? AND content_source_id=? AND state IN ?", tenant, source, models.SourceRunActiveStates).Count(&count).Error
	return count, err
}

func validateReplayUnitAdmission(tx *gorm.DB, unit models.SourceRunExecutionUnit) error {
	var request models.SourceRunRequest
	if err := tx.Where("tenant_id=? AND public_id=?", unit.TenantID, unit.SourceRunRequestID).First(&request).Error; err != nil {
		return err
	}
	if request.Purpose == "content_reset_replay" {
		return fmt.Errorf("Content Reset replay is discontinued")
	}
	return nil
}
