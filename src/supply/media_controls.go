package supply

import (
	"fmt"
	"strings"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// MediaSupplyControlDisabled resolves one fixed, tenant-scoped subtractive
// control. An absent row means the code-default authority remains available;
// callers must still have a separate action contract before writing a row.
func MediaSupplyControlDisabled(db *gorm.DB, tenantID, controlKey string) (bool, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || !models.IsKnownMediaSupplyControlKey(controlKey) {
		return false, fmt.Errorf("media supply control lookup requires a registered key and explicit tenant")
	}
	// Pre-M2A deployments do not have the expanded control contract. Preserve
	// compatibility there; once the table exists, an unreadable lookup is a
	// safety failure for the path that requested it.
	if !db.Migrator().HasTable(&models.MediaSupplyControl{}) {
		return false, nil
	}
	var control models.MediaSupplyControl
	err := db.Where("tenant_id = ? AND control_key = ? AND scope_type = ? AND scope_id = ?", tenantID,
		controlKey, models.MediaSupplyControlScopeTenant, models.MediaSupplyControlScopeAll).First(&control).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// MayScheduleNormalIntake is intentionally narrow. It is not a general
// supply-health gate: receipts, verification, cancellation, and safe
// terminalization never call it.
func MayScheduleNormalIntake(db *gorm.DB, tenantID string) (bool, error) {
	disabled, err := MediaSupplyControlDisabled(db, tenantID, models.MediaSupplyControlNormalIntakeScheduling)
	return !disabled, err
}

// SupplyActionControlKey derives the only supported per-action control name
// from the static registry. Neither a UI nor a model may manufacture this
// identity by concatenating an arbitrary action string.
func SupplyActionControlKey(actionKey string) (string, error) {
	descriptor, ok := SupplyAction(strings.TrimSpace(actionKey))
	if !ok {
		return "", fmt.Errorf("media supply action control requires a registered action")
	}
	return "supply_action:" + descriptor.Key, nil
}

// MayExecuteSupplyAction applies subtractive recovery controls at every
// admission/effect boundary. It deliberately does not govern cancellation,
// receipt retention, reconciliation, verification, or terminalization.
func MayExecuteSupplyAction(db *gorm.DB, tenantID, actionKey string) (bool, string, error) {
	recoveryDisabled, err := MediaSupplyControlDisabled(db, tenantID, models.MediaSupplyControlExceptionalRecovery)
	if err != nil {
		return false, "", err
	}
	if recoveryDisabled {
		return false, models.MediaSupplyControlExceptionalRecovery, nil
	}
	controlKey, err := SupplyActionControlKey(actionKey)
	if err != nil {
		return false, "", err
	}
	actionDisabled, err := MediaSupplyControlDisabled(db, tenantID, controlKey)
	if err != nil {
		return false, "", err
	}
	if actionDisabled {
		return false, controlKey, nil
	}
	return true, "", nil
}
